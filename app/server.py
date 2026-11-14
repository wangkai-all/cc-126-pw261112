"""Dependency-free HTTP server for the shard migration workbench."""

import argparse
import json
import mimetypes
import os
import re
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .engine import Engine, EngineError

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")


def _jsonable(value):
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    return value


class WorkbenchServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, engine: Engine):
        super().__init__(address, WorkbenchHandler)
        self.engine = engine


class WorkbenchHandler(BaseHTTPRequestHandler):
    server_version = "ShardWorkbench/1.0"

    # ------------------------------------------------------------- helpers
    def log_message(self, fmt, *args):
        print("[http] " + fmt % args)

    def _send(self, status, body: bytes, content_type: str):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_json(self, payload, status=200):
        body = json.dumps(_jsonable(payload), ensure_ascii=False).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def send_error_json(self, status, message, detail=None):
        self.send_json({"error": message, "detail": detail}, status)

    def read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise EngineError(f"请求体不是合法 JSON：{exc}") from exc

    def do_GET(self):
        parsed = urlparse(self.path)
        try:
            if parsed.path.startswith("/api/"):
                return self.handle_api("GET", parsed.path, parse_qs(parsed.query))
            return self.serve_static(parsed.path)
        except Exception as exc:  # noqa: BLE001 - surface errors to the page
            traceback.print_exc()
            self.send_error_json(500, str(exc), traceback.format_exc())

    def do_POST(self):
        parsed = urlparse(self.path)
        try:
            return self.handle_api("POST", parsed.path, parse_qs(parsed.query))
        except EngineError as exc:
            self.send_error_json(400, str(exc))
        except KeyError as exc:
            self.send_error_json(404, f"未找到：{exc}")
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self.send_error_json(500, str(exc), traceback.format_exc())

    # -------------------------------------------------------------- static
    def serve_static(self, path: str):
        if path in ("/", ""):
            path = "/index.html"
        rel = os.path.normpath(path.lstrip("/"))
        if rel.startswith("..") or os.path.isabs(rel):
            return self._send(400, b"bad path", "text/plain")
        target = os.path.join(STATIC_DIR, rel)
        if not os.path.isfile(target):
            return self._send(404, b"not found", "text/plain; charset=utf-8")
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        with open(target, "rb") as fh:
            body = fh.read()
        self._send(200, body, ctype + ("; charset=utf-8" if ctype.startswith("text/")
                                       or ctype.endswith("javascript") else ""))

    # ----------------------------------------------------------------- api
    def handle_api(self, method: str, path: str, query: dict):
        engine = self.server.engine
        if method == "GET" and path == "/api/state":
            return self.send_json(engine.state())
        if method == "GET" and path == "/api/customers":
            return self.send_json({"customers": engine.customers()})
        if method == "GET" and path == "/api/query":
            cid = (query.get("customer_id") or [""])[0]
            mode = (query.get("mode") or ["owner"])[0]
            return self.send_json(engine.query(cid, mode=mode))
        if method == "GET" and path == "/api/query-log":
            return self.send_json({"entries": engine.query_log(25)})
        if method == "GET" and path == "/api/audit":
            return self.send_json(engine.audit())
        if method == "GET" and path == "/api/events":
            return self.send_json({"events": engine.events(80)})
        if method == "GET" and path.startswith("/api/migrations/") and path.endswith("/detail"):
            mid = path.split("/")[3]
            return self.send_json(engine.migration_detail(int(mid)))

        body = self.read_json() if method == "POST" else {}

        if method == "POST" and path == "/api/seed":
            return self.send_json(engine.seed(reset=bool(body.get("reset"))))
        if method == "POST" and path == "/api/orders":
            return self.send_json(engine.add_order(
                body.get("customer_id", ""), body.get("amount_cents", 0),
                body.get("note", "")))
        if method == "POST" and path == "/api/migrations":
            return self.send_json(engine.plan_migration(body.get("new_version")))
        m = re.fullmatch(r"/api/migrations/(\d+)/run", path)
        if method == "POST" and m:
            return self.send_json(engine.run(int(m.group(1)),
                                             stop_at_interrupt=bool(body.get("stop_at_interrupt", True))))
        m = re.fullmatch(r"/api/migrations/(\d+)/units/([^/]+)/copy", path)
        if method == "POST" and m:
            return self.send_json(engine.stage_rows(
                int(m.group(1)), m.group(2),
                max_new_rows=body.get("max_new_rows") if body else None))
        m = re.fullmatch(r"/api/migrations/(\d+)/units/([^/]+)/verify", path)
        if method == "POST" and m:
            return self.send_json(engine.verify(int(m.group(1)), m.group(2),
                                                do_switch=bool(body.get("switch"))))
        m = re.fullmatch(r"/api/migrations/(\d+)/units/([^/]+)/cleanup", path)
        if method == "POST" and m:
            return self.send_json(engine.cleanup(int(m.group(1)), m.group(2)))
        m = re.fullmatch(r"/api/migrations/(\d+)/cleanup", path)
        if method == "POST" and m:
            return self.send_json(engine.cleanup_all(int(m.group(1))))
        m = re.fullmatch(r"/api/migrations/(\d+)/units/([^/]+)/conflict", path)
        if method == "POST" and m:
            return self.send_json(engine.inject_conflict(int(m.group(1)), m.group(2)))
        m = re.fullmatch(r"/api/migrations/(\d+)/units/([^/]+)/interrupt", path)
        if method == "POST" and m:
            raise EngineError("请使用复制接口的 max_new_rows 模拟中断")
        if method == "POST" and path == "/api/reset":
            engine.store.reset(2)
            return self.send_json({"reset": True})
        return self.send_error_json(404, f"未知接口 {method} {path}")


def main():
    parser = argparse.ArgumentParser(description="Shard migration workbench")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--data", default=os.environ.get("SHARD_DATA", "data"))
    args = parser.parse_args()
    engine = Engine(args.data)
    engine.seed()
    server = WorkbenchServer((args.host, args.port), engine)
    print(f"Shard migration workbench: http://{args.host}:{args.port}")
    print(f"Data directory: {os.path.abspath(args.data)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
