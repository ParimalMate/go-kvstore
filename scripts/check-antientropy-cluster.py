#!/usr/bin/env python3
"""Verify background repair with real processes, WAL restarts and directed links."""
import http.client
import importlib.util
import json
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("links", ROOT / "scripts/check-vectorclock-cluster.py")
links_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(links_module)


def check(enabled):
    with tempfile.TemporaryDirectory(prefix="kvstore-ae-") as directory:
        work = Path(directory)
        binary = work / "server"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/server"], cwd=ROOT, check=True)
        reservations, links, processes, outputs = [], {}, {}, []
        pause_repair = threading.Event()
        probe_reads = False
        try:
            for _ in range(3):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
            ports = [s.getsockname()[1] for s in reservations]
            for a in range(3):
                for b in range(3):
                    if a != b:
                        links[a, b] = links_module.Link(ports[b])
                        original = links[a, b].server.RequestHandlerClass
                        class RepairGate(original):
                            def do_POST(self):
                                if pause_repair.is_set() and self.path.startswith("/internal/ae/"):
                                    self.rfile.read(int(self.headers.get("Content-Length", "0")))
                                    self.send_response(503)
                                    self.send_header("Content-Length", "0")
                                    self.end_headers()
                                else:
                                    super().do_POST()
                        links[a, b].server.RequestHandlerClass = RepairGate
            for sock in reservations:
                sock.close()

            def request(node, path, body=None, method="GET"):
                connection = http.client.HTTPConnection("127.0.0.1", ports[node], timeout=5)
                try:
                    connection.request(method, path, body=body)
                    response = connection.getresponse()
                    return response.status, response.read().decode()
                finally:
                    connection.close()

            def wait(predicate, description, seconds=20):
                deadline = time.monotonic() + seconds
                while time.monotonic() < deadline:
                    if probe_reads:
                        code, body = request(0, "/kv/key19")
                        assert code == 200 and body == "original", (code, body)
                    if predicate():
                        return
                    time.sleep(0.1)
                raise AssertionError(description)

            def start(node):
                output = open(work / f"n{node + 1}.out", "a")
                outputs.append(output)
                processes[node] = subprocess.Popen([
                    str(binary), "--id", f"n{node + 1}", "--port", str(ports[node]),
                    "--data", str(work / f"n{node + 1}.wal"), "--w", "2", "--r", "2",
                    "--peers", ",".join(links[node, p].address for p in range(3) if p != node),
                    "--ae-interval", "2s" if enabled else "0", "--ae-initial-delay", "1s",
                    "--gossip-interval", "200ms", "--suspect-after", "2s", "--dead-after", "4s",
                ], stdout=output, stderr=subprocess.STDOUT)
                def ready():
                    assert processes[node].poll() is None, "node exited"
                    try:
                        return request(node, "/internal/kv/ready")[0] == 200
                    except (OSError, http.client.HTTPException):
                        return False
                wait(ready, "startup timeout", 5)

            def local(node, key):
                code, body = request(node, "/internal/kv/" + key)
                assert code == 200, (code, body)
                return links_module.normalize(json.loads(body))

            def put(node, key, value):
                code, body = request(node, "/kv/" + key, value, "PUT")
                assert code == 200, (code, body)
                code, body = request(node, "/kv/" + key)
                assert code == 200 and body == value, (code, body)

            def all_equal(keys):
                return all(local(0, k) == local(1, k) == local(2, k) for k in keys)

            for node in range(3):
                start(node)
            keys = [f"key{i}" for i in range(20)]
            for key in keys:
                put(0, key, "original")
            probe_reads = True
            wait(lambda: all_equal(keys), "initial replication")
            if enabled:
                def idle():
                    for node in range(3):
                        code, body = request(node, "/internal/ae/status")
                        assert code == 200
                        reports = json.loads(body)["last_reports"]
                        if not reports or not all(r["roots_equal"] and r["keys_pulled"] == r["keys_pushed"] == 0 for r in reports.values()):
                            return False
                    return True
                wait(idle, "equal-root idle cycles")
            processes[2].kill()
            processes[2].wait()
            changed = [f"new{i}" for i in range(5)] + keys[:2]
            for key in changed:
                put(0, key, "updated")
            # Let outstanding replication finish failing before the restart.
            time.sleep(2.1)
            start(2)
            if enabled:
                wait(lambda: all_equal(keys + changed), "seven missed changes were not repaired")
                print("PASS: restart recovered five missing keys and two overwrites", flush=True)
            else:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    assert not local(2, "new0"), "disabled repair unexpectedly changed state"
                    assert local(2, "key0")[0][0] == "original"
                    time.sleep(0.1)
                print("PASS: disabled scheduling leaves missed writes unrepaired", flush=True)
                return

            # Pause tree exchanges during conflict creation so background
            # repair cannot relay Mumbai through n3 before Pune is created.
            put(0, "city", "Delhi")
            wait(lambda: all_equal(["city"]), "base replication")
            pause_repair.set()
            time.sleep(0.3)
            links[0, 1].blocked.set()
            links[1, 0].blocked.set()
            for node, value in [(0, "Mumbai"), (1, "Pune")]:
                code, body = request(node, "/kv/city", value, "PUT")
                assert code == 200, (code, body)
            mumbai = links_module.normalize([{"value": "Mumbai", "vc": {"n1": 2}}])
            pune = links_module.normalize([{"value": "Pune", "vc": {"n1": 1, "n2": 1}}])
            assert local(0, "city") == mumbai
            assert local(1, "city") == pune
            links[0, 1].blocked.clear()
            links[1, 0].blocked.clear()
            pause_repair.clear()
            wait(lambda: all_equal(["city"]) and local(0, "city") == sorted(mumbai + pune),
                 "concurrent siblings did not converge without quorum reads")
            put(0, "city", "Resolved")
            wait(lambda: all_equal(["city"]) and len(local(0, "city")) == 1, "resolution did not converge")
            print("PASS: background repair preserves both siblings; later PUT resolves them", flush=True)
        except BaseException:
            for path in sorted(work.glob("*.out")):
                print(path.name + ":\n" + "\n".join(path.read_text().splitlines()[-25:]), flush=True)
            raise
        finally:
            for p in processes.values():
                if p.poll() is None:
                    p.terminate()
            for p in processes.values():
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()
                    p.wait()
            for link in links.values():
                link.close()
            for sock in reservations:
                sock.close()
            for output in outputs:
                output.close()


if __name__ == "__main__":
    check(True)
    check(False)
    print("PASS: real-process anti-entropy checks", flush=True)
