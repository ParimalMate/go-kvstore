#!/usr/bin/env python3
"""Check real three-node conflicts using directed, test-only HTTP link proxies."""

import http.client
import json
import socket
import subprocess
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class Link:
    """One source node's path to one destination; no OS firewall changes."""

    def __init__(self, destination):
        self.blocked = threading.Event()
        self.rejected = threading.Event()
        link = self

        class Proxy(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                connection = None
                try:
                    body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                    if link.blocked.is_set():
                        link.rejected.set()
                        status, payload = 503, b"test link blocked"
                        content_type = "text/plain"
                    else:
                        connection = http.client.HTTPConnection("127.0.0.1", destination, timeout=4)
                        connection.request(self.command, self.path, body=body,
                                           headers={"Content-Type": self.headers.get("Content-Type", "application/json")})
                        response = connection.getresponse()
                        status, payload = response.status, response.read()
                        content_type = response.getheader("Content-Type", "text/plain")
                    self.send_response(status)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                except (OSError, http.client.HTTPException):
                    # The node treats a failed proxy exchange as a failed peer.
                    self.close_connection = True
                finally:
                    if connection is not None:
                        connection.close()

            do_GET = forward
            do_PUT = forward

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True)
        self.thread.start()

    @property
    def address(self):
        return f"127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


def normalize(versions):
    return sorted((v["value"], tuple(sorted(v["vc"].items()))) for v in versions)


def main():
    with tempfile.TemporaryDirectory(prefix="kvstore-vectorclock-") as directory:
        work = Path(directory)
        binary = work / "server"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/server"], cwd=ROOT, check=True)
        reservations, links, processes, outputs = [], {}, [], []
        try:
            # Keep node ports reserved while proxies acquire their own ports.
            for _ in range(3):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                reservations.append(sock)
            ports = [sock.getsockname()[1] for sock in reservations]
            for source in range(3):
                for target in range(3):
                    if source != target:
                        links[source, target] = Link(ports[target])
            for sock in reservations:
                sock.close()

            def request(node, path, value=None):
                connection = http.client.HTTPConnection("127.0.0.1", ports[node], timeout=5)
                try:
                    connection.request("GET" if value is None else "PUT", path, body=value)
                    response = connection.getresponse()
                    return response.status, response.read().decode(), response.getheader("Content-Type")
                finally:
                    connection.close()

            def local(node, key):
                status, body, _ = request(node, "/internal/kv/" + key)
                assert status == 200, (node, status, body)
                return json.loads(body)

            def wait_local(node, key, expected):
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if normalize(local(node, key)) == normalize(expected):
                        return
                    time.sleep(0.02)
                raise AssertionError((node, key, local(node, key), expected))

            def put(node, key, value):
                status, body, _ = request(node, "/kv/" + key, value)
                assert status == 200 and body == "OK", (node, status, body)
                print(f"PUT via n{node + 1}: {key}={value} -> 200 (W=2)", flush=True)

            for node in range(3):
                output = open(work / f"n{node + 1}.out", "w")
                outputs.append(output)
                process = subprocess.Popen([
                    str(binary), "--id", f"n{node + 1}", "--port", str(ports[node]),
                    "--data", str(work / f"n{node + 1}.wal"), "--w", "2", "--r", "2",
                    "--peers", ",".join(links[node, peer].address for peer in range(3) if peer != node),
                ], stdout=output, stderr=subprocess.STDOUT)
                processes.append(process)
                deadline = time.monotonic() + 5
                while True:
                    assert process.poll() is None, f"n{node + 1} exited"
                    try:
                        if request(node, "/internal/kv/ready")[0] == 200:
                            break
                    except (OSError, http.client.HTTPException):
                        pass
                    assert time.monotonic() < deadline, "startup timeout"
                    time.sleep(0.02)

            # Ordinary sequential writes are still one clean value.
            put(0, "normal", "first")
            put(0, "normal", "second")
            normal = [{"value": "second", "vc": {"n1": 2}}]
            for node in range(3):
                wait_local(node, "normal", normal)
                status, body, _ = request(node, "/kv/normal")
                assert status == 200 and body == "second", (status, body)
            print("Sequential path: 3/3 quorum GETs returned 200 second", flush=True)

            # Establish a common causal base before splitting the coordinators.
            put(0, "city", "Delhi")
            base = [{"value": "Delhi", "vc": {"n1": 1}}]
            for node in range(3):
                wait_local(node, "city", base)
            links[0, 1].blocked.set()
            links[1, 0].blocked.set()
            print("Partition enabled: n1 <-> n2 blocked; both still reach n3", flush=True)
            put(0, "city", "Mumbai")
            # n3 cannot relay Mumbai to n2: internal writes do not re-fan-out.
            assert normalize(local(1, "city")) == normalize(base)
            put(1, "city", "Pune")
            a = {"value": "Mumbai", "vc": {"n1": 2}}
            b = {"value": "Pune", "vc": {"n1": 1, "n2": 1}}
            expected_local = [[a], [b], [a, b]]
            for node in range(3):
                wait_local(node, "city", expected_local[node])
            for edge in [(0, 1), (1, 0)]:
                assert links[edge].rejected.wait(3), f"blocked link {edge} was not exercised"
            print("Partitioned stores: " + json.dumps([local(i, "city") for i in range(3)]), flush=True)

            links[0, 1].blocked.clear()
            links[1, 0].blocked.clear()
            print("Partition healed: all six directed peer links restored", flush=True)
            for node in range(3):
                for _ in range(5):
                    status, body, content_type = request(node, "/kv/city")
                    assert status == 300 and content_type == "application/json", (node, status, body)
                    assert normalize(json.loads(body)) == normalize([a, b]), body
                print(f"n{node + 1}: 5/5 quorum GETs returned 300 with exact Mumbai/Pune clocks", flush=True)
            # Reconnection/reads have not delivered missing versions into local stores.
            for node in range(3):
                assert normalize(local(node, "city")) == normalize(expected_local[node])
                status, body, _ = request(node, "/kv/normal")
                assert status == 200 and body == "second", (status, body)
            print("No read repair: n1 still has only Mumbai; n2 only Pune; n3 both", flush=True)
            print("Sequential key remains 200 second on all three coordinators", flush=True)
            print("PASS: real three-node partition and non-conflicting path", flush=True)
        except BaseException:
            for path in sorted(work.glob("*.out")):
                print(f"{path.name} log tail:\n" + "\n".join(path.read_text().splitlines()[-25:]), flush=True)
            raise
        finally:
            for process in processes:
                if process.poll() is None:
                    process.terminate()
            for process in processes:
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
