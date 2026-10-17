#!/usr/bin/env python3
"""Verify gossip POST transport through the partition-test proxy."""

import http.client
import importlib.util
import queue
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "vectorclock_cluster", Path(__file__).with_name("check-vectorclock-cluster.py")
)
cluster = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cluster)


class GossipProxyTest(unittest.TestCase):
    def test_post_forward_block_and_recover(self):
        received = queue.Queue()

        class Destination(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                received.put((self.path, body))
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Destination)
        thread = threading.Thread(target=server.serve_forever,
                                  kwargs={"poll_interval": 0.05}, daemon=True)
        thread.start()
        link = None
        try:
            link = cluster.Link(server.server_port)
            payload = b'{"from":"n1","members":[]}'

            def post():
                connection = http.client.HTTPConnection(link.address, timeout=5)
                try:
                    connection.request("POST", "/internal/gossip", body=payload,
                                       headers={"Content-Type": "application/json"})
                    response = connection.getresponse()
                    return response.status, response.read()
                finally:
                    connection.close()

            self.assertEqual(post(), (200, payload))
            self.assertEqual(received.get(timeout=1), ("/internal/gossip", payload))
            link.blocked.set()
            self.assertEqual(post()[0], 503)
            self.assertTrue(link.rejected.is_set())
            self.assertTrue(received.empty())
            link.blocked.clear()
            self.assertEqual(post(), (200, payload))
            self.assertEqual(received.get(timeout=1), ("/internal/gossip", payload))
        finally:
            if link is not None:
                link.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
