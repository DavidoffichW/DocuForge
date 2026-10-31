from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from docuforge.app import create_app
from docuforge.config import Settings


def main() -> None:
    parser = argparse.ArgumentParser(description="Start the local DocuForge editor")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--workspace", type=Path)
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"}:
        parser.error("DocuForge editor foundation is loopback-only")
    settings = Settings.from_environment(args.workspace)
    uvicorn.run(create_app(settings), host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
