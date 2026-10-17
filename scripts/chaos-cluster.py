#!/usr/bin/env python3
"""Seeded fault-injection harness for the real three-process cluster.

Runs writers and readers while a scheduler kills, pauses, restarts, partitions
and slows nodes; then heals everything and checks the invariants in
chaos_invariants.py. A seed fixes the fault schedule and each writer's operation
sequence. Real-process timing still varies, so it is a replayable schedule, not
a deterministic simulation.

  python3 scripts/chaos-cluster.py --seed 1 --duration 60
  python3 scripts/chaos-cluster.py --control no-repair     # must DETECT divergence
  python3 scripts/chaos-cluster.py --mutate-final          # must DETECT tampered state

Exit codes: 0 all invariants held (or the control worked), 1 violations, 2 harness error.
"""
import argparse
import json
import os
import random
import shutil
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import chaos_invariants as inv  # noqa: E402
import chaos_schedule as sched  # noqa: E402
import clusterlib as cl  # noqa: E402

PAYLOADS = ["", " ", "ünï☃ 日本語", "line1\nline2", "a b  c", "x" * 1024, "{\"json\": [1, 2]}"]
CLIENT_TIMEOUT = 5.0  # above the server's 2s quorum timeout, so ordinary slowness is not indeterminate
LOG_PATTERNS = ("panic:", "fatal error:", "WARNING: DATA RACE")


class History:
    """Attempts by (key, seq). A pending placeholder is registered BEFORE sending, so a
    read that observes an in-flight write is not mistaken for an invented value."""

    def __init__(self):
        self.lock = threading.Lock()
        self.attempts = {}

    def begin(self, key, seq, value, start):
        with self.lock:
            self.attempts[(key, seq)] = inv.Attempt(key, seq, value, start, start, "indeterminate")

    def finish(self, attempt):
        with self.lock:
            self.attempts[(attempt.key, attempt.seq)] = attempt

    def snapshot(self):
        with self.lock:
            return list(self.attempts.values())


class Run:
    def __init__(self, args, out_dir):
        self.args = args
        self.out_dir = out_dir
        self.history = History()
        self.stop = threading.Event()
        self.lock = threading.Lock()
        self.counts = Counter()
        self.violations = []
        self.event_log = open(out_dir / "events.jsonl", "a", buffering=1)
        self.keys = [f"w{w}-k{j}" for w in range(args.writers) for j in range(args.keys_per_writer)]

    def count(self, name):
        with self.lock:
            self.counts[name] += 1

    def violation(self, kind, detail):
        with self.lock:
            self.violations.append({"kind": kind, "detail": detail, "at": round(time.monotonic(), 3)})
        self.log("violation", kind=kind, detail=str(detail)[:500])

    def log(self, event, **fields):
        record = {"wall": round(time.time(), 3), "mono": round(time.monotonic(), 3), "event": event, **fields}
        with self.lock:
            self.event_log.write(json.dumps(record, ensure_ascii=False) + "\n")


def make_value(rng, key, seq):
    return f"{key}|{seq}|{rng.getrandbits(32):08x}|{rng.choice(PAYLOADS)}"


def put_once(run, cluster, node_index, key, seq, value):
    start = time.monotonic()
    run.history.begin(key, seq, value, start)
    try:
        status, _ = cl.request(cluster.nodes[node_index].port, "PUT", cl.key_path("/kv", key), value, CLIENT_TIMEOUT)
        attempt = inv.Attempt(key, seq, value, start, time.monotonic(),
                              "acked" if status == 200 else "rejected", status)
    except (cl.RequestTimeout, cl.ConnectionFailed):
        attempt = inv.Attempt(key, seq, value, start, time.monotonic(), "indeterminate")
    run.history.finish(attempt)
    run.count(f"put_{attempt.outcome}")
    for message in inv.check_put(attempt):
        run.violation("put", f"{key} seq {seq}: {message}")
    return attempt


def writer_loop(run, cluster, index):
    rng = random.Random(f"{run.args.seed}:writer:{index}")
    keys = [f"w{index}-k{j}" for j in range(run.args.keys_per_writer)]
    seqs = {key: 1 for key in keys}  # warmup wrote seq 1
    while not run.stop.is_set():
        key = rng.choice(keys)
        seqs[key] += 1
        value = make_value(rng, key, seqs[key])
        put_once(run, cluster, rng.randrange(len(cluster.nodes)), key, seqs[key], value)
        time.sleep(rng.uniform(0.0, 0.05))


def reader_loop(run, cluster, index):
    rng = random.Random(f"{run.args.seed}:reader:{index}")
    while not run.stop.is_set():
        key = rng.choice(run.keys)
        node = cluster.nodes[rng.randrange(len(cluster.nodes))]
        started = time.monotonic()
        minimum = inv.floor(run.history.snapshot(), key, started)
        try:
            status, body = cl.request(node.port, "GET", cl.key_path("/kv", key), None, CLIENT_TIMEOUT)
        except (cl.RequestTimeout, cl.ConnectionFailed):
            run.count("get_error")
            time.sleep(0.02)
            continue
        ended = time.monotonic()
        run.count(f"get_{status}")
        values = ()
        try:
            if status == 200:
                values = [body.decode("utf-8")]
            elif status == 300:
                values = [v["value"] for v in json.loads(body)]
        except (UnicodeDecodeError, ValueError, KeyError, TypeError):
            run.violation("read", f"{key} via {node.id}: undecodable {status} response")
            continue
        for message in inv.check_read(run.history.snapshot(), key, started, status, values,
                                      ended=ended, minimum=minimum):
            run.violation("read", f"{key} via {node.id} status {status}: {message}")
        time.sleep(rng.uniform(0.0, 0.05))


class Injector(threading.Thread):
    def __init__(self, run, cluster, events, t0):
        super().__init__(daemon=True)
        self.run_, self.cluster, self.events, self.t0 = run, cluster, events, t0
        self.applied = Counter()

    def run(self):
        for event in self.events:
            wait = self.t0 + event.at - time.monotonic()
            if wait > 0 and self.run_.stop.wait(wait):
                return
            try:
                self.apply(event)
                self.applied[event.action] += 1
                self.run_.log("fault", at=event.at, action=event.action, args=list(event.args))
            except Exception as exc:  # a failed fault is reported, never silently skipped
                self.run_.log("fault_error", at=event.at, action=event.action, error=repr(exc))
                self.run_.count("fault_errors")

    def apply(self, event):
        a, args, c = event.action, event.args, self.cluster
        if a == "kill":
            c.node(args[0]).kill9()
        elif a == "revive":
            c.node(args[0]).start()
        elif a == "pause":
            c.node(args[0]).pause()
        elif a == "resume":
            c.node(args[0]).resume()
        elif a == "bounce":
            node = c.node(args[0])
            code = node.stop()
            if code != 0:
                self.run_.violation("graceful-stop", f"{node.id} exit code {code} after SIGTERM")
            node.start()
        elif a == "cut":
            c.link(args[0], args[1]).blocked.set()
            c.link(args[1], args[0]).blocked.set()
        elif a == "uncut":
            c.link(args[0], args[1]).blocked.clear()
            c.link(args[1], args[0]).blocked.clear()
        elif a == "oneway":
            c.link(args[0], args[1]).blocked.set()
        elif a == "unoneway":
            c.link(args[0], args[1]).blocked.clear()
        elif a == "slow":
            c.link(args[0], args[1]).delay = args[2] / 1000.0
        elif a == "unslow":
            c.link(args[0], args[1]).delay = 0.0
        else:
            raise ValueError(f"unknown fault {a}")


def wait_roots_equal(cluster, timeout, stable=3):
    """Poll until every node reports the same root hash `stable` times in a row."""
    start = time.monotonic()
    streak, roots = 0, {}
    while time.monotonic() - start < timeout:
        try:
            roots = {node.id: cluster.root_hash(node) for node in cluster.nodes}
            streak = streak + 1 if len(set(roots.values())) == 1 else 0
        except (cl.ConnectionFailed, cl.RequestTimeout, ValueError, KeyError):
            streak = 0
        if streak >= stable:
            return True, round(time.monotonic() - start, 2), roots
        time.sleep(0.5)
    return False, round(time.monotonic() - start, 2), roots


def fetch_states(cluster, keys):
    states = {node.id: {} for node in cluster.nodes}
    for node in cluster.nodes:
        for key in keys:
            states[node.id][key] = cluster.siblings(node, key)
    return states


def scan_logs(cluster):
    findings = []
    for node in cluster.nodes:
        text = node.log_text()
        for pattern in LOG_PATTERNS:
            if pattern in text:
                findings.append(f"{node.id} log contains {pattern!r}")
    return findings


def warmup(run, cluster):
    rng = random.Random(f"{run.args.seed}:warmup")
    for key in run.keys:
        attempt = put_once(run, cluster, 0, key, 1, make_value(rng, key, 1))
        if attempt.outcome != "acked":
            raise RuntimeError(f"warmup write for {key} was not acknowledged: {attempt}")


def summarize(run, cluster, extra):
    ae = {}
    for node in cluster.nodes:
        try:
            status, body = cl.request(node.port, "GET", "/internal/ae/status", None, 3)
            data = json.loads(body)
            ae[node.id] = data.get("totals", data)
        except Exception as exc:
            ae[node.id] = {"error": repr(exc)}
    summary = {"seed": run.args.seed, "duration": run.args.duration, "counts": dict(run.counts),
               "violations": run.violations[:50], "violation_count": len(run.violations),
               "anti_entropy": ae, "race_build": cluster.race_enabled, **extra}
    (run.out_dir / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    return summary


def print_summary(run, summary):
    c = summary["counts"]
    print(f"\nseed {summary['seed']}  duration {summary['duration']}s  race build: {summary['race_build']}")
    print(f"  schedule hash      {summary.get('schedule_hash')}   faults applied: {summary.get('faults')}")
    print(f"  writes             acked {c.get('put_acked', 0)}  rejected {c.get('put_rejected', 0)}  indeterminate {c.get('put_indeterminate', 0)}")
    reads = {k[4:]: v for k, v in c.items() if k.startswith('get_')}
    print(f"  reads              {reads}")
    print(f"  convergence        {summary.get('convergence_seconds')}s   restart replay identical: {summary.get('replay_identical')}")
    print(f"  violations         {summary['violation_count']}")
    for v in summary["violations"][:10]:
        print(f"    - [{v['kind']}] {v['detail'][:200]}")


def execute(args, out_dir, workdir, mode="chaos"):
    """Run one cluster lifecycle and return (summary, run, cluster states)."""
    run = Run(args, out_dir)
    cluster = Cluster = cl.Cluster(workdir, out_dir / "logs", anti_entropy=not args.no_ae, race=not args.no_race)
    extra = {}
    try:
        cluster.build()
        cluster.start()
        if not cluster.wait_ready():
            run.violation("startup", "cluster did not report all nodes alive")
        warmup(run, cluster)
        ok, seconds, _ = wait_roots_equal(cluster, 15)
        if not ok:
            run.violation("warmup", "roots did not converge after warmup")

        events = sched.build_schedule(args.seed, args.duration)
        extra["schedule_hash"] = sched.schedule_hash(events)
        print(f"seed {args.seed}: {len(events)} scheduled fault events, schedule hash {extra['schedule_hash']}")

        threads = [threading.Thread(target=writer_loop, args=(run, cluster, i), daemon=True) for i in range(args.writers)]
        threads += [threading.Thread(target=reader_loop, args=(run, cluster, i), daemon=True) for i in range(args.readers)]
        t0 = time.monotonic()
        injector = Injector(run, cluster, events, t0)
        for t in threads + [injector]:
            t.start()
        while time.monotonic() - t0 < args.duration:
            for node in cluster.nodes:
                if node.crashed():
                    run.violation("process", f"{node.id} exited unexpectedly with code {node.proc.returncode}")
                    node.state = "down"
            time.sleep(0.25)

        run.stop.set()
        for t in threads + [injector]:
            t.join(timeout=15)
        if any(t.is_alive() for t in threads):
            run.violation("harness", "a workload thread did not finish within 15s")
        extra["faults"] = dict(injector.applied)

        run.log("heal")
        cluster.heal()
        cluster.wait_ready(timeout=30)
        converged, seconds, roots = wait_roots_equal(cluster, 40)
        extra["convergence_seconds"] = seconds
        if not converged:
            run.violation("convergence", f"roots still differ after {seconds}s: {roots}")

        states = fetch_states(cluster, run.keys)
        for message in inv.check_final(run.history.snapshot(), states):
            run.violation("final", message)
        if args.mutate_final:
            return control_mutate(run, cluster, states, extra)

        snapshot = {n: {k: inv.canonical(v) for k, v in s.items()} for n, s in states.items()}
        run.log("restart-replay")
        for node in cluster.nodes:
            code = node.stop()
            if code != 0:
                run.violation("graceful-stop", f"{node.id} exit code {code} on shutdown")
        for node in cluster.nodes:
            node.start()
        cluster.wait_ready(timeout=40)
        converged, _, roots = wait_roots_equal(cluster, 40)
        replayed = fetch_states(cluster, run.keys) if converged else {}
        identical = converged and all(
            inv.canonical(replayed[n][k]) == snapshot[n][k] for n in snapshot for k in snapshot[n])
        extra["replay_identical"] = bool(identical)
        if not identical:
            run.violation("replay", "state after a clean restart differs from the state before it")

        for finding in scan_logs(cluster):
            run.violation("log", finding)
        summary = summarize(run, cluster, extra)
        for node_id, totals in summary["anti_entropy"].items():
            if isinstance(totals, dict) and totals.get("collisions"):
                run.violation("anti-entropy", f"{node_id} reported clock collisions: {totals['collisions']}")
        summary = summarize(run, cluster, extra)
        print_summary(run, summary)
        return summary, run, None
    finally:
        cluster.shutdown()
        run.event_log.close()


def control_mutate(run, cluster, states, extra):
    """Tamper with the REAL collected final state; the checker must notice."""
    real_errors = list(run.violations)
    attempts = run.history.snapshot()
    acked = {}
    for a in attempts:
        if a.outcome == "acked":
            acked[a.key] = max(acked.get(a.key, 0), a.seq)
    victim = next((k for k in sorted(acked) if acked[k] > 1), None) or sorted(acked)[0]
    mangled = next(k for k in sorted(acked) if k != victim)
    tampered = {n: {k: [dict(v) for v in vs] for k, vs in s.items()} for n, s in states.items()}
    for s in tampered.values():
        s[victim] = [v for v in s[victim] if not v["value"].startswith(f"{victim}|{acked[victim]}|")]
        if s[mangled]:
            s[mangled][0]["value"] += "-MANGLED"
    errors = inv.check_final(attempts, tampered)
    lost = any("lost acknowledged write" in e for e in errors)
    invented = any("invented value" in e for e in errors)
    clean = not real_errors
    print(f"mutate-final: real run clean={clean}  lost-write detected={lost}  invented-value detected={invented}")
    ok = clean and lost and invented
    print("CONTROL OK: the final-state checker flags tampered data" if ok else "CONTROL FAILED")
    return {"control": "mutate-final", "ok": ok}, run, None


def control_no_repair(args, out_dir, workdir):
    """Anti-entropy off: a node that missed writes must NOT converge, and the checker must say so."""
    args.no_ae = True
    run = Run(args, out_dir)
    cluster = cl.Cluster(workdir, out_dir / "logs", anti_entropy=False, race=not args.no_race)
    try:
        cluster.build()
        cluster.start()
        cluster.wait_ready()
        keys = run.keys[:12]
        rng = random.Random("control")
        for key in keys:
            put_once(run, cluster, 0, key, 1, make_value(rng, key, 1))
        cluster.node("n3").kill9()
        for key in run.keys[12:17]:  # new keys written while n3 is down
            put_once(run, cluster, 0, key, 1, make_value(rng, key, 1))
        for key in keys[:2]:  # and two overwrites
            put_once(run, cluster, 0, key, 2, make_value(rng, key, 2))
        cluster.node("n3").start()
        cluster.wait_ready()
        converged, seconds, roots = wait_roots_equal(cluster, 10)
        states = fetch_states(cluster, keys + run.keys[12:17])
        errors = inv.check_final(run.history.snapshot(), states)
        disagree = [e for e in errors if "nodes disagree" in e]
        print(f"no-repair control: converged={converged} after {seconds}s; checker reported {len(errors)} problems, {len(disagree)} disagreements")
        ok = (not converged) and len(disagree) >= 7
        print("CONTROL OK: divergence without anti-entropy is detected" if ok else "CONTROL FAILED: divergence went undetected")
        return 0 if ok else 1
    finally:
        cluster.shutdown()
        run.event_log.close()


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--duration", type=float, default=60.0, help="seconds of chaos")
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--readers", type=int, default=2)
    p.add_argument("--keys-per-writer", type=int, default=20)
    p.add_argument("--no-ae", action="store_true", help="disable anti-entropy (expect non-convergence)")
    p.add_argument("--no-race", action="store_true", help="build the server without the race detector")
    p.add_argument("--out", default="chaos-results")
    p.add_argument("--control", choices=["no-repair"])
    p.add_argument("--mutate-final", action="store_true")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    out_dir = Path(args.out) / (f"control-{args.control}" if args.control else
                                f"seed-{args.seed}{'-mutate' if args.mutate_final else ''}")
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True)
    workdir = tempfile.mkdtemp(prefix="chaos-")
    try:
        if args.control == "no-repair":
            return control_no_repair(args, out_dir, workdir)
        summary, run, _ = execute(args, out_dir, workdir)
        if args.mutate_final:
            return 0 if summary.get("ok") else 1
        return 0 if summary["violation_count"] == 0 else 1
    except KeyboardInterrupt:
        print("interrupted")
        return 2
    except Exception:
        traceback.print_exc()
        return 2
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
