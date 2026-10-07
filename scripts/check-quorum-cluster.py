#!/usr/bin/env python3
"""Exercise quorum behavior using isolated real processes and temporary WALs."""

import json
import signal
import socket
import statistics
import subprocess
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def main():
    with tempfile.TemporaryDirectory(prefix="kvstore-quorum-") as directory:
        work = Path(directory)
        binary = work / "server"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/server"], cwd=ROOT, check=True)
        # Reserve distinct ephemeral ports until all three have been chosen.
        sockets = [socket.socket() for _ in range(3)]
        try:
            for sock in sockets:
                sock.bind(("127.0.0.1", 0))
            ports = [sock.getsockname()[1] for sock in sockets]
        finally:
            for sock in sockets:
                sock.close()

        processes = [None] * 3
        outputs = []

        def request(node, path, value=None):
            command = ["curl", "--silent", "--show-error", "--noproxy", "*",
                       "--max-time", "4", "-w", "\n%{http_code}\n%{time_total}"]
            if value is not None:
                command += ["-X", "PUT", "--data-binary", value]
            command.append(f"http://127.0.0.1:{ports[node]}{path}")
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            body, status, elapsed = result.stdout.rsplit("\n", 2)
            return int(status), body, float(elapsed)

        def start(node):
            output = open(work / f"n{node + 1}.out", "a")
            outputs.append(output)
            processes[node] = subprocess.Popen([
                str(binary), "--id", f"n{node + 1}", "--port", str(ports[node]),
                "--data", str(work / f"n{node + 1}.wal"),
                "--peers", ",".join(f"127.0.0.1:{port}" for i, port in enumerate(ports) if i != node),
                "--w", "2", "--r", "2",
            ], stdout=output, stderr=subprocess.STDOUT)
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if processes[node].poll() is not None:
                    raise RuntimeError(f"n{node + 1} exited during startup")
                try:
                    if request(node, "/internal/kv/ready")[0] == 200:
                        return
                except subprocess.CalledProcessError:
                    pass
                time.sleep(0.05)
            raise RuntimeError(f"n{node + 1} did not become ready")

        def metadata(node, key):
            status, body, _ = request(node, f"/internal/kv/{key}")
            assert status == 200, (node, status, body)
            versions = json.loads(body)
            assert isinstance(versions, list), versions
            if not versions:
                return {"value": "", "vc": {}}
            assert len(versions) == 1, versions
            return versions[0]

        def wait_value(node, key, value):
            deadline = time.monotonic() + 4
            while time.monotonic() < deadline:
                if metadata(node, key)["value"] == value:
                    return
                time.sleep(0.05)
            raise AssertionError(f"n{node + 1} did not receive {value!r}")

        def timed_writes(label):
            timings = []
            for attempt in range(5):
                status, body, elapsed = request(0, "/kv/latency", f"{label}-{attempt}")
                assert status == 200, (label, status, body)
                timings.append(elapsed)
            print(f"{label}: 5/5 PUTs returned 200; median={statistics.median(timings):.6f}s; max={max(timings):.6f}s", flush=True)
            return timings

        try:
            for node in range(3):
                start(node)
            baseline = timed_writes("all healthy")

            processes[2].send_signal(signal.SIGSTOP)
            try:
                paused = timed_writes("n3 paused")
                assert max(paused) < 1.0, "paused node held up a quorum write"
                assert statistics.median(paused) < statistics.median(baseline) + 0.25, "large latency regression"
            finally:
                processes[2].send_signal(signal.SIGCONT)
            wait_value(2, "latency", "n3 paused-4")

            status, _, _ = request(0, "/kv/gap", "old")
            assert status == 200
            for node in range(3):
                wait_value(node, "gap", "old")

            processes[2].kill()
            processes[2].wait(timeout=5)
            timed_writes("n3 killed")
            status, _, elapsed = request(0, "/kv/gap", "fresh")
            assert status == 200
            print(f"write during outage: 200 in {elapsed:.6f}s", flush=True)
            # Let all failed replication attempts finish before restarting n3.
            time.sleep(2.1)
            start(2)
            before = [metadata(node, "gap") for node in range(3)]
            assert [item["value"] for item in before] == ["fresh", "fresh", "old"], before
            assert before[0]["vc"] == before[1]["vc"] and before[0]["vc"]["n1"] > before[2]["vc"]["n1"], before
            print("local versions before quorum reads: " + json.dumps(before), flush=True)
            for node in range(3):
                for _ in range(5):
                    status, body, _ = request(node, "/kv/gap")
                    assert status == 200 and body == "fresh", (node, status, body)
            assert metadata(2, "gap")["value"] == "old", "reads unexpectedly repaired the stale replica"
            print("quorum GETs: 15/15 returned fresh, including reads coordinated by stale n3", flush=True)
            print("n3 remains locally stale: confirmed reads select without repair", flush=True)
            time.sleep(0.1)
            print("Example n3 read logs:", flush=True)
            lines = (work / "n3.out").read_text().splitlines()
            selected = [line for line in lines if 'read key="gap" ->' in line or 'READ quorum reached key="gap"' in line]
            print("\n".join(selected[:6]), flush=True)
            print("PASS: all real-cluster checks", flush=True)
        except BaseException:
            for node in range(3):
                path = work / f"n{node + 1}.out"
                if path.exists():
                    print(f"n{node + 1} log tail:\n" + "\n".join(path.read_text().splitlines()[-25:]), flush=True)
            raise
        finally:
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
            for output in outputs:
                output.close()


if __name__ == "__main__":
    main()
