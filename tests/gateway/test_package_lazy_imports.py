"""Behavior contracts for the lightweight gateway package boundary."""

import json
import os
import subprocess
import sys


def test_lightweight_status_import_does_not_initialize_gateway_stack():
    repo = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, sys; import gateway.status; "
            "print(json.dumps(sorted(name for name in sys.modules "
            "if name in {'gateway.config','gateway.session','gateway.delivery'})))",
        ],
        cwd=repo,
        env={**os.environ, "PYTHONPATH": repo},
        text=True,
        capture_output=True,
        timeout=15,
        check=True,
    )
    assert json.loads(probe.stdout.strip().splitlines()[-1]) == []


def test_public_gateway_exports_still_resolve_lazily():
    import gateway
    from gateway.config import GatewayConfig

    assert gateway.GatewayConfig is GatewayConfig
