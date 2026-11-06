"""westock 自用 HTTP API。

用法:
  python -m data_sourcing.westock_api.server        # 默认 127.0.0.1:8090
  WESTOCK_API_PORT=8090 python -m data_sourcing.westock_api.server

接口:
  GET  /health                     健康检查（westock 是否可用）
  GET  /api/v1/<子命令路径>?flag=v   透传: /api/v1/kline/sh600519?period=day&limit=20
                                   → westock kline sh600519 --period day --limit 20
  POST /api/exec                   {"args": ["kline", "sh600519", "--period", "day"]}
"""
from __future__ import annotations

import os

from flask import Flask, jsonify, request

from .client import DEFAULT_TIMEOUT, find_westock, run_westock

app = Flask(__name__)


@app.get("/health")
def health():
    exe = find_westock()
    return jsonify({"ok": exe is not None, "westock": exe})


@app.post("/api/exec")
def exec_cmd():
    body = request.get_json(silent=True) or {}
    args = body.get("args")
    if not isinstance(args, list) or not args or not all(isinstance(a, str) and a.strip() for a in args):
        return jsonify({"ok": False, "error": 'body 需为 {"args": ["子命令", ...]}'}), 400
    return jsonify(run_westock(args))


@app.get("/api/v1/<path:subpath>")
def passthrough(subpath: str):
    args = [seg for seg in subpath.split("/") if seg]
    for key, values in request.args.lists():
        for value in values:
            args += [f"--{key}", value]
    return jsonify(run_westock(args))


@app.errorhandler(ValueError)
def on_value_error(e):
    return jsonify({"ok": False, "error": str(e)}), 400


@app.errorhandler(FileNotFoundError)
def on_not_found(e):
    return jsonify({"ok": False, "error": str(e)}), 503


def main():
    host = os.environ.get("WESTOCK_API_HOST", "127.0.0.1")
    port = int(os.environ.get("WESTOCK_API_PORT", "8090"))
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
