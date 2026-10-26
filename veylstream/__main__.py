"""CLI: python -m veylstream <serve|setup-demo|bench>"""
from __future__ import annotations

import argparse
import os
import secrets
import sys

from .runtime import Config, Runtime


def setup_demo(rt: Runtime, email: str, password: str | None, seed: bool) -> None:
    ws = rt.store.q1("SELECT id FROM workspaces WHERE name='Demo workspace'")
    created_pw = None
    if ws is None:
        wid = rt.create_workspace("Demo workspace")
        rt.store.set_settings(wid, {"demo": True})
    else:
        wid = ws["id"]
    if rt.store.q1("SELECT 1 x FROM users WHERE email=?", (email.lower(),)) is None:
        created_pw = password or secrets.token_urlsafe(14)
        rt.create_user(email, created_pw, "Demo Admin", wid, "admin")
        print(f"created admin {email}\npassword (shown once): {created_pw}")
    else:
        print(f"user {email} already exists; password unchanged")
    if seed:
        print("seeding 30 minutes of SIMULATED history through the real pipeline ...")
        print(rt.demo(wid).seed(30, 20))
    print(f"workspace id: {wid}")


def main() -> int:
    ap = argparse.ArgumentParser(prog="veylstream")
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("serve")
    s.add_argument("--host", default=os.environ.get("VEYLSTREAM_HOST", "127.0.0.1"))
    s.add_argument("--port", type=int, default=int(os.environ.get("VEYLSTREAM_PORT", "8080")))
    d = sub.add_parser("setup-demo")
    d.add_argument("--email", default=os.environ.get("VEYLSTREAM_ADMIN_EMAIL", "admin@veylstream.local"))
    d.add_argument("--password", default=os.environ.get("VEYLSTREAM_ADMIN_PASSWORD"))
    d.add_argument("--no-seed", action="store_true")
    a = ap.parse_args()
    rt = Runtime(Config.from_env())
    try:
        if a.cmd == "setup-demo":
            setup_demo(rt, a.email, a.password, not a.no_seed)
        elif a.cmd == "serve":
            from werkzeug.serving import make_server
            from .api import create_app
            srv = make_server(a.host, a.port, create_app(rt), threaded=True)
            print(f"Veylstream {__import__('veylstream').__version__} listening on http://{a.host}:{a.port}  (local single-process profile)")
            try:
                srv.serve_forever()
            except KeyboardInterrupt:
                print("shutting down ...")
    finally:
        rt.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
