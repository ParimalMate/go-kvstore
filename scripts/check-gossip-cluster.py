#!/usr/bin/env python3
"""Verify gossip with real processes, directed links, and concurrent quorum traffic."""

import http.client
import importlib.util
import json
import signal
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "vectorclock_cluster", Path(__file__).with_name("check-vectorclock-cluster.py")
)
cluster = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster)


def main():
    with tempfile.TemporaryDirectory(prefix="kvstore-gossip-") as directory:
        work = Path(directory)
        binary = work / "server"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/server"], cwd=ROOT, check=True)
        reservations, links, processes, outputs = [], {}, [None] * 3, []
        traffic_stop = threading.Event()
        traffic_lock = threading.Lock()
        traffic_state = {"node": 0, "count": 0, "errors": []}
        traffic_thread = None
        try:
            for _ in range(3):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
            ports = [sock.getsockname()[1] for sock in reservations]
            for source in range(3):
                for target in range(3):
                    if source != target:
                        links[source, target] = cluster.Link(ports[target])
            for sock in reservations:
                sock.close()

            def request(node, path, value=None):
                connection = http.client.HTTPConnection("127.0.0.1", ports[node], timeout=3)
                try:
                    connection.request("GET" if value is None else "PUT", path, body=value)
                    response = connection.getresponse()
                    return response.status, response.read().decode()
                finally:
                    connection.close()

            def view(node):
                status, body = request(node, "/internal/members")
                assert status == 200, (node, status, body)
                return json.loads(body)

            def member(observer, target):
                return next((m for m in view(observer)["members"] if m["id"] == f"n{target + 1}"), None)

            def has_persisted(node):
                status, body = request(node, "/internal/kv/persist")
                versions = json.loads(body) if status == 200 else []
                return len(versions) == 1 and versions[0]["value"] == "durable"

            def check_traffic():
                with traffic_lock:
                    assert not traffic_state["errors"], traffic_state["errors"]

            def wait_for(description, condition, timeout=8):
                deadline = time.monotonic() + timeout
                while True:
                    check_traffic()
                    if condition():
                        print("PASS: " + description, flush=True)
                        return
                    assert time.monotonic() < deadline, "timeout: " + description
                    time.sleep(0.1)

            def start(node):
                output = open(work / f"n{node + 1}.out", "a")
                outputs.append(output)
                processes[node] = subprocess.Popen([
                    str(binary), "--id", f"n{node + 1}", "--port", str(ports[node]),
                    "--data", str(work / f"n{node + 1}.wal"), "--w", "2", "--r", "2", "--ae-interval", "0",
                    "--peers", ",".join(links[node, peer].address for peer in range(3) if peer != node),
                    "--gossip-interval", "200ms", "--suspect-after", "2s", "--dead-after", "4s",
                ], stdout=output, stderr=subprocess.STDOUT)
                def ready():
                    assert processes[node].poll() is None, f"n{node + 1} exited"
                    try:
                        return request(node, "/internal/members")[0] == 200
                    except (OSError, http.client.HTTPException):
                        return False
                wait_for(f"n{node + 1} started", ready)

            def all_alive():
                return all(len(v["members"]) == 3 and all(m["status"] == "alive" for m in v["members"])
                           for v in [view(i) for i in range(3)])

            def traffic():
                sequence = 0
                while not traffic_stop.is_set():
                    # Holding this lock lets a scenario switch coordinators only
                    # after the previous probe finished, before cutting its links.
                    with traffic_lock:
                        node = traffic_state["node"]
                        try:
                            path, value = f"/kv/probe-{sequence}", f"value-{sequence}"
                            assert request(node, path, value) == (200, "OK"), "PUT failed"
                            assert request(node, path) == (200, value), "GET failed"
                            traffic_state["count"] += 1
                        except Exception as error:
                            traffic_state["errors"].append(f"n{node + 1}: {error!r}")
                            return
                    sequence += 1
                    traffic_stop.wait(0.2)

            def detect_dead(target, observers):
                seen = {observer: [] for observer in observers}
                def detected():
                    for observer in observers:
                        status = member(observer, target)["status"]
                        if not seen[observer] or seen[observer][-1] != status:
                            seen[observer].append(status)
                    return all(states[-1] == "dead" for states in seen.values())
                # Bound is dead-after plus 600ms of observation/scheduling margin.
                began = time.monotonic()
                wait_for(f"n{target + 1} becomes dead", detected, timeout=4.6)
                elapsed = time.monotonic() - began
                assert elapsed <= 4.6, f"detection exceeded 4.6s bound: {elapsed:.3f}s"
                for states in seen.values():
                    assert "suspect" in states and states.index("suspect") < states.index("dead"), seen
                print(f"  observed suspect then dead in {elapsed:.3f}s: {seen}", flush=True)

            for node in range(3):
                start(node)
            wait_for("three nodes converge to alive", all_alive)
            # Persist a known key on n2 so restart also verifies WAL continuity.
            assert request(0, "/kv/persist", "durable") == (200, "OK")
            wait_for("n2 holds persisted value", lambda: has_persisted(1))
            traffic_thread = threading.Thread(target=traffic, daemon=True)
            traffic_thread.start()

            old_generation = view(1)["generation"]
            processes[1].kill()
            processes[1].wait(timeout=5)
            detect_dead(1, [0, 2])
            start(1)
            assert view(1)["generation"] > old_generation
            assert has_persisted(1)
            wait_for("restart is alive with newer generation", lambda: all_alive() and
                     all(member(i, 1)["generation"] > old_generation for i in [0, 2]))

            paused_generation = view(2)["generation"]
            processes[2].send_signal(signal.SIGSTOP)
            detect_dead(2, [0, 1])
            processes[2].send_signal(signal.SIGCONT)
            wait_for("resumed n3 is alive with the same generation", lambda: all_alive() and
                     all(member(i, 2)["generation"] == paused_generation for i in range(3)))

            links[0, 2].blocked.set()
            links[2, 0].blocked.set()
            initial_hb = member(0, 2)["heartbeat"]
            until = time.monotonic() + 5  # Longer than dead-after.
            while time.monotonic() < until:
                check_traffic()
                assert member(0, 2)["status"] == "alive", "indirect evidence failed"
                time.sleep(0.1)
            assert member(0, 2)["heartbeat"] > initial_hb
            print("PASS: blocked n1/n3 link stays alive through n2 for over dead-after", flush=True)

            with traffic_lock:
                traffic_state["node"] = 1  # n2 and n3 retain quorum during n1 isolation.
            links[0, 1].blocked.set()
            links[1, 0].blocked.set()
            wait_for("isolated n1 reports both peers dead", lambda:
                     all(member(0, i)["status"] == "dead" for i in [1, 2]), timeout=4.6)
            for link in links.values():
                link.blocked.clear()
            wait_for("healed cluster returns to all alive", all_alive)
            traffic_stop.set()
            traffic_thread.join(timeout=5)
            assert not traffic_thread.is_alive()
            check_traffic()
            print(f"PASS: {traffic_state['count']} PUT/GET pairs returned 200 during all scenarios", flush=True)
            print("PASS: real-process gossip verification", flush=True)
        except BaseException:
            for path in sorted(work.glob("*.out")):
                print(f"{path.name} log tail:\n" + "\n".join(path.read_text().splitlines()[-25:]), flush=True)
            raise
        finally:
            traffic_stop.set()
            if traffic_thread is not None:
                traffic_thread.join(timeout=7)
            for process in processes:
                if process is not None and process.poll() is None:
                    process.send_signal(signal.SIGCONT)
                    process.terminate()
            for process in processes:
                if process is not None:
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for link in links.values():
                link.close()
            for sock in reservations:
                sock.close()
            for output in outputs:
                output.close()


if __name__ == "__main__":
    main()
