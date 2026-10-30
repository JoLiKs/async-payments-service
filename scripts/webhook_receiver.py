import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        code = 500 if self.path.startswith("/fail") else 200
        print(f"webhook {self.path} -> {code}: {json.loads(body or b'{}')}", flush=True)
        self.send_response(code)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass


if __name__ == "__main__":
    port = int(os.getenv("PORT", "9000"))
    print(f"listening on :{port}", flush=True)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
