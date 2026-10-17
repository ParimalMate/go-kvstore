import json
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

import clusterlib as cl


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        out = json.dumps({"method": self.command, "path": self.path, "body": body.decode()}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)

    do_GET = do_PUT = do_POST = respond


class LinkTests(unittest.TestCase):
    def setUp(self):
        self.backend = HTTPServer(("127.0.0.1", 0), Backend)
        threading.Thread(target=self.backend.serve_forever, daemon=True).start()
        self.link = cl.Link(self.backend.server_address[1], "a->b")

    def tearDown(self):
        self.link.close()
        self.backend.shutdown()
        self.backend.server_close()

    def test_forwards_get_put_and_post_with_bodies(self):
        for method in ("GET", "PUT", "POST"):
            status, body = cl.request(self.link.port, method, "/x/y", "héllo\nworld" if method != "GET" else None)
            data = json.loads(body)
            self.assertEqual((status, data["method"], data["path"]), (200, method, "/x/y"))
            if method != "GET":
                self.assertEqual(data["body"], "héllo\nworld")

    def test_blocked_link_answers_503_until_unblocked(self):
        self.link.blocked.set()
        self.assertEqual(cl.request(self.link.port, "GET", "/")[0], 503)
        self.link.blocked.clear()
        self.assertEqual(cl.request(self.link.port, "GET", "/")[0], 200)

    def test_delay_slows_requests(self):
        self.link.delay = 0.3
        start = time.monotonic()
        cl.request(self.link.port, "GET", "/")
        self.assertGreaterEqual(time.monotonic() - start, 0.3)

    def test_dead_upstream_becomes_503_not_a_hang(self):
        self.backend.shutdown()
        self.backend.server_close()
        self.assertEqual(cl.request(self.link.port, "GET", "/")[0], 503)
        self.setUp()  # so tearDown has something to close


class RequestClassificationTests(unittest.TestCase):
    def test_refused_connection_is_connection_failed(self):
        with self.assertRaises(cl.ConnectionFailed):
            cl.request(cl.free_port(), "GET", "/", timeout=1)

    def test_slow_server_is_a_timeout_not_a_connection_failure(self):
        class Slow(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_GET(self):
                time.sleep(1.0)

        server = HTTPServer(("127.0.0.1", 0), Slow)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with self.assertRaises(cl.RequestTimeout):
                cl.request(server.server_address[1], "GET", "/", timeout=0.2)
        finally:
            server.shutdown()
            server.server_close()

    def test_unicode_bodies_are_sent_as_utf8(self):
        backend = HTTPServer(("127.0.0.1", 0), Backend)
        threading.Thread(target=backend.serve_forever, daemon=True).start()
        try:
            status, body = cl.request(backend.server_address[1], "PUT", "/k", "ünï☃ 日本語")
            self.assertEqual(json.loads(body)["body"], "ünï☃ 日本語")
        finally:
            backend.shutdown()
            backend.server_close()

    def test_key_path_escapes_everything(self):
        self.assertEqual(cl.key_path("/kv", "a/b c"), "/kv/a%2Fb%20c")


class NodeStateTests(unittest.TestCase):
    """Regression: the chaos monitor once reported a deliberate graceful stop as a crash,
    because the node's state flipped to 'down' only AFTER the process had already exited."""

    class Sleeper(cl.Node):
        def command(self):
            return ["sleep", "30"]

        def wait_listening(self, timeout=15):
            pass

    def make(self):
        return self.Sleeper(0, 1, "x", tempfile.mkdtemp(), tempfile.mkdtemp(), [], [], {})

    def watch_while(self, action):
        node, seen = self.make(), []
        node.start()
        stop_watching = threading.Event()

        def watch():
            while not stop_watching.is_set():
                if node.crashed():
                    seen.append(True)

        thread = threading.Thread(target=watch)
        thread.start()
        action(node)
        stop_watching.set()
        thread.join()
        return seen

    def test_graceful_stop_is_never_seen_as_a_crash(self):
        for _ in range(25):
            self.assertEqual(self.watch_while(lambda n: n.stop()), [])

    def test_kill9_is_never_seen_as_a_crash(self):
        for _ in range(25):
            self.assertEqual(self.watch_while(lambda n: n.kill9()), [])

    def test_a_real_unexpected_exit_IS_seen_as_a_crash(self):
        node = self.make()
        node.start()
        node.proc.kill()  # something outside the harness kills it
        node.proc.wait()
        self.assertTrue(node.crashed())

    def test_stopping_a_paused_node_still_works(self):
        node = self.make()
        node.start()
        node.pause()
        self.assertEqual(node.state, "paused")
        self.assertIsNotNone(node.stop())
        self.assertEqual(node.state, "down")


if __name__ == "__main__":
    unittest.main()
