"""Private subprocess entry point for aq.init(); reuse the normal server lifecycle."""
import json
from hashlib import sha256
import os
from pathlib import Path
import socket
import sys
from uuid import uuid4

from filelock import FileLock


def main() -> None:
    root = Path(sys.argv[1]).resolve()
    directory = root / ".runtime"
    with FileLock(directory / "server.lock", timeout=0):
        from acquirium.cli import _load_config, _apply_server_env

        explicit_config = len(sys.argv) > 2
        config = Path(sys.argv[2]) if explicit_config else directory / "server.toml"
        if not explicit_config:
            config.write_text("")
        os.environ["ACQUIRIUM_CONFIG"] = str(config)
        cfg = _load_config(config)
        _apply_server_env(cfg)
        # Resolve modules next to the config as the CLI does for driver specs.
        sys.path.insert(0, str(config.parent))
        port = int(cfg.get("server", {}).get("port", 0))
        driver = cfg.get("driver", {})
        if (driver.get("server_url", "127.0.0.1") not in {"127.0.0.1", "localhost", "0.0.0.0"}
                or driver.get("use_ssl", False)
                or driver.get("server_port", port) != port):
            raise ValueError("local runtime driver address must match the local server port and use HTTP")
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", port))
            port = listener.getsockname()[1]
            os.environ["ACQUIRIUM_SELF_PORT"] = str(port)

            import uvicorn
            from acquirium.Server.app import app

            info = {"port": port, "token": uuid4().hex,
                    "config": sha256(json.dumps(cfg, sort_keys=True, default=str).encode()).hexdigest() if explicit_config else None,
                    "exact_only": os.environ["ACQUIRIUM_EXACT_ONLY"] == "true"}

            @app.get("/_local_runtime", include_in_schema=False)
            def local_runtime():
                return info

            registry = directory / "server.json"
            temporary = directory / "server.json.tmp"
            temporary.write_text(json.dumps(info))
            temporary.replace(registry)
            try:
                server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port))
                server.run(sockets=[listener])
            finally:
                registry.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
