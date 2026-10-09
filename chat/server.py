"""Local chat app: asks questions of call records and the Databricks-written Sales table, both read through Lakehouse shortcuts.

Required environment variables (nothing is hard coded; set them in your shell profile, never in the repo):
  FABRIC_TENANT_ID, FABRIC_CLIENT_ID     service principal with access to the Fabric workspace
  FABRIC_CLIENT_SECRET (or my_secret)    its secret; never commit or print it
  FABRIC_WORKSPACE, FABRIC_LAKEHOUSE     workspace and lakehouse names that hold the shortcuts
  ONELAKE_HOST                           OneLake DFS host for your cloud, for example <prefix>-onelake.dfs.fabric.microsoft.us
  AZURE_OPENAI_ENDPOINT, AZURE_OPENAI_API_KEY
Optional: AZURE_OPENAI_DEPLOYMENT (gpt-4o), SALES_TABLE (dbo/sales), CHAT_PORT (8000), CHAT_REFRESH_SECONDS (300)

Run: python -m chat.server            (serves http://127.0.0.1:8000)
     python -m chat.server --check    (reads the shortcut and prints what it found; no model needed)
"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from chat import engine, source

PAGE = Path(__file__).with_name("index.html")
LOGO = Path(__file__).resolve().parent.parent / "images" / "Fabricks.svg"
MAX_BODY = 4096
MAX_QUESTION = 500


class State:
    def __init__(self, src, refresh_seconds):
        self.src, self.refresh_seconds = src, refresh_seconds
        self.lock = threading.Lock()
        self.conn = self.provenance = None
        self.loaded = 0.0

    def data(self, force=False):
        with self.lock:
            if force or self.conn is None or time.time() - self.loaded > self.refresh_seconds:
                self.conn, self.provenance = source.load(self.src)
                self.loaded = time.time()
            return self.conn, self.provenance


def require_env(name):
    value = os.environ.get(name)
    if not value:
        sys.exit(f"Set the {name} environment variable (for example in ~/.bashrc)")
    return value


def build_source():
    secret = os.environ.get("FABRIC_CLIENT_SECRET") or os.environ.get("my_secret")
    if not secret:
        sys.exit("Set FABRIC_CLIENT_SECRET (or my_secret)")
    tenant, client = require_env("FABRIC_TENANT_ID"), require_env("FABRIC_CLIENT_ID")
    return source.ShortcutSource(
        require_env("ONELAKE_HOST"),
        require_env("FABRIC_WORKSPACE"),
        require_env("FABRIC_LAKEHOUSE"),
        tenant,
        source.make_credential(tenant, client, secret),
        sales_table=os.getenv("SALES_TABLE", "dbo/sales"),
    )


def make_handler(state, client, deployment):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status, body, content_type="application/json"):
            payload = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; script-src 'unsafe-inline'")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/":
                self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif self.path == "/logo.svg" and LOGO.is_file():
                self._send(200, LOGO.read_bytes(), "image/svg+xml")
            else:
                self._send(404, {"error": "not found"})

        def do_POST(self):
            if self.path not in ("/api/ask", "/api/refresh"):
                return self._send(404, {"error": "not found"})
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if not 0 <= length <= MAX_BODY:
                    return self._send(413, {"error": "request too large"})
                body = json.loads(self.rfile.read(length) or b"{}")
            except ValueError:
                return self._send(400, {"error": "invalid JSON"})
            try:
                if self.path == "/api/refresh":
                    _, provenance = state.data(force=True)
                    return self._send(200, {k: v for k, v in provenance.items() if k != "files"})
                question = str(body.get("question", "")).strip()
                if not question or len(question) > MAX_QUESTION:
                    return self._send(400, {"error": f"question must be 1-{MAX_QUESTION} characters"})
                conn, provenance = state.data()
                self._send(200, engine.ask(client, deployment, conn, provenance, question))
            except Exception as exc:  # report the class only; never echo credentials or payloads
                print(f"request failed: {type(exc).__name__}", file=sys.stderr)
                self._send(500, {"error": f"{type(exc).__name__} while answering; see server log"})

        def log_message(self, fmt, *args):
            pass

    return Handler


def main():
    src = build_source()
    state = State(src, int(os.getenv("CHAT_REFRESH_SECONDS", "300")))
    if "--check" in sys.argv:
        _, provenance = state.data(force=True)
        print(json.dumps({k: v for k, v in provenance.items() if k != "files"}, indent=2))
        for item in provenance["files"][:3]:
            print("read:", item["path"])
        return
    from openai import AzureOpenAI

    client = AzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_OPENAI_API_KEY"],
        api_version="2024-12-01-preview",
    )
    state.data(force=True)
    port = int(os.getenv("CHAT_PORT", "8000"))
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(state, client, os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")))
    print(f"Serving http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
