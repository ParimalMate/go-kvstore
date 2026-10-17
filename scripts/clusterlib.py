"""Process and network helpers for the real-cluster scripts and the chaos harness.

Starts real server processes behind per-direction proxies so tests can kill,
pause, restart, partition and slow nodes. Standard library only.
"""
import atexit
import http.client
import json
import os
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import quote

ROOT = Path(__file__).resolve().parents[1]


class ConnectionFailed(Exception):
    """The request may or may not have reached the server."""


class RequestTimeout(Exception):
    """No complete response arrived in time; the request may still apply later."""


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def request(port, method, path, body=None, timeout=5.0):
    """Return (status, body bytes). Raises RequestTimeout or ConnectionFailed."""
    if isinstance(body, str):
        body = body.encode("utf-8")
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        conn.request(method, path, body=body)
        response = conn.getresponse()
        return response.status, response.read()
    except (socket.timeout, TimeoutError) as exc:
        raise RequestTimeout(str(exc)) from exc
    except (OSError, http.client.HTTPException) as exc:
        raise ConnectionFailed(str(exc)) from exc
    finally:
        conn.close()


def key_path(prefix, key):
    return f"{prefix}/{quote(key, safe='')}"


class Link:
    """Directed proxy: the source node dials this port to reach the destination."""

    def __init__(self, target_port, name):
        self.name = name
        self.target_port = target_port
        self.blocked = threading.Event()
        self.delay = 0.0
        link = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def forward(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else None
                if link.delay:
                    time.sleep(link.delay)
                if link.blocked.is_set():
                    return self.reply(503, b"")
                try:
                    conn = http.client.HTTPConnection("127.0.0.1", link.target_port, timeout=10)
                    headers = {k: v for k, v in self.headers.items() if k.lower() == "content-type"}
                    conn.request(self.command, self.path, body=body, headers=headers)
                    upstream = conn.getresponse()
                    data = upstream.read()
                    self.reply(upstream.status, data, upstream.getheader("Content-Type"))
                    conn.close()
                except Exception:
                    self.reply(503, b"")

            def reply(self, status, data, content_type=None):
                try:
                    self.send_response(status)
                    if content_type:
                        self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            do_GET = do_PUT = do_POST = forward

        class Server(ThreadingHTTPServer):
            daemon_threads = True
            request_queue_size = 128

            def handle_error(self, request, client_address):
                pass  # a client that hung up mid-reply (timeout, kill) is expected here

        self.server = Server(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def address(self):
        return f"127.0.0.1:{self.port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class Node:
    """One server process with a fixed id, port and WAL, so restarts replay state."""

    def __init__(self, index, port, binary, workdir, logdir, peers, flags, env):
        self.id = f"n{index + 1}"
        self.port = port
        self.binary = binary
        self.wal = Path(workdir) / f"{self.id}.wal"
        self.log_path = Path(logdir) / f"{self.id}.log"
        self.peers = peers
        self.flags = flags
        self.env = env
        self.proc = None
        self.state = "down"  # up, paused, down
        self.starts = 0

    def command(self):
        return [str(self.binary), "--id", self.id, "--port", str(self.port),
                "--data", str(self.wal), "--peers", ",".join(self.peers), *self.flags]

    def start(self):
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.log_path, "ab") as log:
            log.write(f"=== start #{self.starts + 1} ===\n".encode())
            self.proc = subprocess.Popen(self.command(), stdout=log, stderr=subprocess.STDOUT, env=self.env)
        self.starts += 1
        self.state = "up"
        self.wait_listening()

    def wait_listening(self, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"{self.id} exited during startup with code {self.proc.returncode}")
            try:
                with socket.create_connection(("127.0.0.1", self.port), timeout=0.5):
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError(f"{self.id} did not start listening")

    def alive(self):
        return self.proc is not None and self.proc.poll() is None

    def crashed(self):
        """Exited although the harness did not stop it.

        Order matters: observe the exit FIRST, then read the state. A deliberate stop sets
        state to 'down' before it signals the process, so once an exit is visible the state
        is already 'down'. Reading the state first leaves a window that misreads a stop as a crash.
        """
        if self.proc is None or self.proc.poll() is None:
            return False
        return self.state in ("up", "paused")

    def kill9(self):
        self.state = "down"  # set BEFORE signalling so the crash monitor never mistakes this for a crash
        if self.alive():
            self.proc.send_signal(signal.SIGKILL)
        if self.proc is not None:
            self.proc.wait()

    def pause(self):
        if self.alive():
            self.proc.send_signal(signal.SIGSTOP)
            self.state = "paused"

    def resume(self):
        if self.alive():
            self.proc.send_signal(signal.SIGCONT)
            self.state = "up"

    def stop(self, timeout=20):
        """Graceful SIGTERM. Returns the exit code, or None if it had to be killed."""
        was_paused = self.state == "paused"
        self.state = "down"  # set BEFORE signalling, see kill9
        if not self.alive():
            return self.proc.returncode if self.proc else None
        if was_paused:
            self.proc.send_signal(signal.SIGCONT)
        self.proc.send_signal(signal.SIGTERM)
        try:
            return self.proc.wait(timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            return None

    def log_text(self):
        try:
            return self.log_path.read_text(errors="replace")
        except OSError:
            return ""


class Cluster:
    def __init__(self, workdir, logdir, n=3, w=2, r=2, anti_entropy=True, race=True):
        self.workdir = Path(workdir)
        self.logdir = Path(logdir)
        self.n, self.w, self.r = n, w, r
        self.anti_entropy = anti_entropy
        self.race = race
        self.binary = self.workdir / "kvserver"
        self.nodes = []
        self.links = {}
        self.race_enabled = False
        self._closed = False
        atexit.register(self.shutdown)

    def build(self):
        self.workdir.mkdir(parents=True, exist_ok=True)
        base = [str(self.binary), "./cmd/server"]
        if self.race:
            result = subprocess.run(["go", "build", "-race", "-o", *base], cwd=ROOT, capture_output=True, text=True)
            if result.returncode == 0:
                self.race_enabled = True
                return
            print("WARNING: race build failed, falling back to a normal build:\n" + result.stderr[-400:])
        result = subprocess.run(["go", "build", "-o", *base], cwd=ROOT, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("go build failed:\n" + result.stderr)

    def start(self):
        ports = [free_port() for _ in range(self.n)]
        for i in range(self.n):
            for j in range(self.n):
                if i != j:
                    self.links[(i, j)] = Link(ports[j], f"n{i + 1}->n{j + 1}")
        env = dict(os.environ)
        if self.race_enabled:
            env["GORACE"] = "halt_on_error=1 exitcode=66"
        flags = ["--w", str(self.w), "--r", str(self.r),
                 "--gossip-interval", "200ms", "--suspect-after", "2s", "--dead-after", "4s"]
        flags += ["--ae-interval", "2s", "--ae-initial-delay", "1s"] if self.anti_entropy else ["--ae-interval", "0"]
        for i in range(self.n):
            peers = [self.links[(i, j)].address for j in range(self.n) if j != i]
            self.nodes.append(Node(i, ports[i], self.binary, self.workdir, self.logdir, peers, flags, env))
        for node in self.nodes:
            node.start()

    def node(self, name):
        return next(node for node in self.nodes if node.id == name)

    def index(self, name):
        return int(name[1:]) - 1

    def link(self, src, dst):
        return self.links[(self.index(src), self.index(dst))]

    def wait_ready(self, timeout=40):
        """Every running node reports every node alive through gossip."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(self._sees_all_alive(node) for node in self.nodes if node.alive()):
                return True
            time.sleep(0.25)
        return False

    def _sees_all_alive(self, node):
        try:
            status, body = request(node.port, "GET", "/internal/members", timeout=2)
            members = json.loads(body)["members"]
        except (ConnectionFailed, RequestTimeout, ValueError, KeyError):
            return False
        return status == 200 and len(members) == self.n and all(m["status"] == "alive" for m in members)

    def root_hash(self, node):
        status, body = request(node.port, "POST", "/internal/ae/nodes",
                               json.dumps({"level": 0, "indexes": [0]}), timeout=5)
        if status != 200:
            raise ConnectionFailed(f"root hash status {status}")
        return json.loads(body)["hashes"][0]

    def siblings(self, node, key):
        status, body = request(node.port, "GET", key_path("/internal/kv", key), timeout=5)
        if status != 200:
            raise ConnectionFailed(f"internal GET status {status}")
        return json.loads(body)

    def heal(self):
        """Undo every injected fault and bring every node back."""
        for link in self.links.values():
            link.blocked.clear()
            link.delay = 0.0
        for node in self.nodes:
            if node.state == "paused":
                node.resume()
            if not node.alive():
                node.start()
            else:
                node.state = "up"

    def shutdown(self):
        """Always safe to call twice. Never leaves paused or orphaned servers behind."""
        if self._closed:
            return
        self._closed = True
        for node in self.nodes:
            try:
                if node.proc is not None and node.proc.poll() is None:
                    node.proc.send_signal(signal.SIGCONT)
                    node.proc.kill()
                    node.proc.wait(timeout=5)
            except Exception:
                pass
        for link in self.links.values():
            try:
                link.close()
            except Exception:
                pass
