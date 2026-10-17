#!/usr/bin/env python3
"""Loopback benchmarks; normal builds, sequential clients, durable writes.

python3 scripts/bench.py > docs/BENCHMARKS.md
Use --only B3 for the quorum payoff (200 timed-out W=3 requests take ~400s).
"""
import argparse
import contextlib
import datetime
import json
import math
import os
import platform
import statistics
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import clusterlib as cl


def info(*cmd):
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unavailable"


class BenchCluster(cl.Cluster):
    """Direct peer connections: fault proxies are unnecessary for latency tests."""
    def start(self):
        reservations=[]
        try:
            for _ in range(self.n):
                sock=socket.socket()
                sock.bind(("127.0.0.1",0))
                reservations.append(sock)
            ports=[sock.getsockname()[1] for sock in reservations]
            flags=["--w",str(self.w),"--r",str(self.r),"--gossip-interval","200ms",
                   "--suspect-after","2s","--dead-after","4s"]
            flags += ["--ae-interval","2s","--ae-initial-delay","1s"] if self.anti_entropy else ["--ae-interval","0"]
            for i,port in enumerate(ports):
                peers=[f"127.0.0.1:{p}" for j,p in enumerate(ports) if j!=i]
                self.nodes.append(cl.Node(i,port,self.binary,self.workdir,self.logdir,peers,flags,dict(os.environ)))
            for sock,node in zip(reservations,self.nodes):
                sock.close()
                node.start()
        finally:
            for sock in reservations:sock.close()


@contextlib.contextmanager
def cluster(n=3, w=2, r=2, ae=False):
    with tempfile.TemporaryDirectory(prefix="kvstore-bench-") as directory:
        c = BenchCluster(Path(directory)/"work", Path(directory)/"logs", n=n, w=w, r=r,
                       anti_entropy=ae, race=False)
        try:
            c.build()
            c.start()
            assert c.wait_ready(), "gossip readiness failed"
            yield c
        except BaseException:
            destination=Path("verify-logs")/f"bench-failure-{time.time_ns()}"
            if c.logdir.exists():shutil.copytree(c.logdir,destination)
            print(f"Benchmark failure logs: {destination}",file=__import__('sys').stderr)
            raise
        finally:
            c.shutdown()


def wait_equal(c, timeout=40):
    start = time.monotonic()
    while time.monotonic()-start < timeout:
        roots = [c.root_hash(n) for n in c.nodes]
        if len(set(roots)) == 1:
            return time.monotonic()-start
        time.sleep(0.05)
    raise RuntimeError("roots did not converge")


def put(c, key, value):
    code, body = cl.request(c.nodes[0].port, "PUT", cl.key_path("/kv",key),value)
    assert code == 200, (code,body)


def measure(c, count, method, prefix, allow_failure=False):
    durations, codes = [], {}
    for i in range(count):
        start = time.perf_counter()
        code, body = cl.request(c.nodes[0].port,method,cl.key_path("/kv",f"{prefix}-{i}"),
                                "x"*64 if method=="PUT" else None,timeout=5)
        durations.append((time.perf_counter()-start)*1000)
        codes[code] = codes.get(code,0)+1
        assert code == 200 or (allow_failure and code == 503), (code,body)
        if method=="GET": assert body==b"x"*64
    return durations,codes


def latency_row(name, result):
    values,codes=result
    ordered=sorted(values)
    percentiles=[ordered[max(0,math.ceil(len(ordered)*p)-1)] for p in (.5,.95,.99)]
    print(f"| {name} | {len(values)} | "+" | ".join(f"{v:.3f}" for v in percentiles)+
          " | "+", ".join(f"{k}: {v}" for k,v in sorted(codes.items()))+" |",flush=True)


def latency_header():
    print("| Case | Requests | p50 ms | p95 ms | p99 ms | HTTP outcomes |\n|---|---:|---:|---:|---:|---|",flush=True)


def basic(number,n,w,r):
    print(f"\n## {number}: {n} node(s), W={w}, R={r}\n",flush=True)
    latency_header()
    with cluster(n,w,r) as c:
        latency_row("PUT, 64 bytes",measure(c,2000,"PUT","basic"))
        latency_row("GET",measure(c,2000,"GET","basic"))


def quorum():
    print("\n## B3: quorum payoff with one paused peer\n",flush=True)
    print("Latency includes failed requests; HTTP outcomes distinguish success from timeouts.\n",flush=True)
    latency_header()
    for w,r in [(2,2),(3,1)]:
        with cluster(w=w,r=r) as c:
            latency_row(f"W={w}, R={r}, healthy",measure(c,200,"PUT","healthy"))
            c.nodes[2].pause()
            try:
                latency_row(f"W={w}, R={r}, n3 paused",measure(c,200,"PUT","paused",True))
            finally:
                c.nodes[2].resume()


def efficiency():
    print("\n## B4: Merkle comparison after 10,000 shared keys\n",flush=True)
    print("Automatic repair is disabled; manual cycles isolate the measured transfer.\n"
          "Hashes exclude the final verification root; requests include it.\n",flush=True)
    print("| Divergent keys | Hashes fetched | Requests | Duration ms | Pulled | Roots equal after | Naive key comparisons |\n|---:|---:|---:|---:|---:|---|---:|",flush=True)
    with cluster() as c:
        for i in range(10000): put(c,f"key-{i}","initial")
        wait_equal(c)
        for count in [1,10,100]:
            c.nodes[2].kill9()
            for i in range(count): put(c,f"key-{i}",f"changed-{count}")
            # Outstanding replication failures must finish before restarting C.
            time.sleep(2.1)
            c.nodes[2].start()
            peer=f"127.0.0.1:{c.nodes[0].port}"
            code,body=cl.request(c.nodes[2].port,"POST","/internal/ae/run",json.dumps({"peer":peer}),timeout=65)
            assert code==200,(code,body)
            report=json.loads(body)[0]
            assert not report["error"] and report["keys_pulled"]==count and report["roots_equal_after"],report
            print(f"| {count} | {report['hashes_fetched']} | {report['requests']} | {report['duration_ms']} | {report['keys_pulled']} | yes | 10000 |",flush=True)


def recovery():
    print("\n## B5: automatic catch-up after 100 missed writes\n",flush=True)
    print("Repair interval 2s/initial delay 1s; gossip 200ms/2s/4s. "
          "Elapsed time starts before restart and ends at the first equal-root poll.\n",flush=True)
    print("| Repetition | Recovery seconds |\n|---:|---:|",flush=True)
    values=[]
    with cluster(ae=True) as c:
        for repetition in range(1,6):
            c.nodes[2].kill9()
            for i in range(100): put(c,f"recovery-{repetition}-{i}","value")
            time.sleep(2.1)
            start=time.monotonic()
            c.nodes[2].start()
            wait_equal(c)
            elapsed=time.monotonic()-start
            values.append(elapsed)
            print(f"| {repetition} | {elapsed:.3f} |",flush=True)
    print(f"\nMedian recovery: **{statistics.median(values):.3f}s**.",flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only",choices=["B1","B2","B3","B4","B5"])
    args=parser.parse_args()
    print("# Loopback benchmarks\n")
    print("Indicative educational measurements, not production capacity claims. "
          "Sequential Python HTTP clients open a connection per request; peer traffic uses "
          "direct HTTP connections, without fault proxies. Server builds omit the race detector. Writes append and fsync the WAL; "
          "latency includes client, OS and storage costs. No throughput extrapolation is made.\n")
    print(f"- Date (UTC): {datetime.datetime.now(datetime.timezone.utc).isoformat()}")
    print(f"- OS: {platform.platform()}")
    print(f"- CPU: {info('sysctl','-n','machdep.cpu.brand_string')} ({os.cpu_count()} logical CPUs)")
    print(f"- Go: {info('go','version')}")
    print(f"- Python: {platform.python_version()}")
    print(f"- Commit: {info('git','rev-parse','--short','HEAD')}")
    print("- Percentiles: nearest rank; no client concurrency or warmup exclusion.\n",flush=True)
    for name,fn in [("B1",lambda:basic("B1",1,1,1)),("B2",lambda:basic("B2",3,2,2)),
                    ("B3",quorum),("B4",efficiency),("B5",recovery)]:
        if args.only is None or args.only==name:fn()


if __name__=="__main__":
    main()
