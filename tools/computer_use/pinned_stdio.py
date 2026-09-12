"""Explicit, integrity-checked legacy transport for user-pinned drivers.

This is not the modern private-daemon contract. It is only available when
Hermes already resolved unrestricted authorization and no capability ceiling
was requested. Bounded/standard modes must keep their native approval path.
"""
import hashlib
import os
from pathlib import Path


def enabled(config, permission_mode, driver_cmd):
    receipt = config.get("pinned_stdio_transport")
    if not receipt:
        return False
    if not isinstance(receipt, dict):
        raise ValueError("pinned_stdio_transport requires an explicit artifact receipt")
    if permission_mode != "unrestricted" or config.get("capability_manifest"):
        raise ValueError("Pinned stdio transport requires unrestricted authorization and no capability manifest; legacy drivers cannot enforce modern scoped grants")
    version = receipt.get("version")
    if not version or str(config.get("driver_version_pin")) != version:
        raise ValueError("Pinned stdio receipt must match driver_version_pin exactly")
    files = receipt.get("files")
    if not isinstance(files, dict) or not files:
        raise ValueError("Pinned stdio receipt requires file SHA-256 identities")
    resolved = os.path.realpath(driver_cmd)
    if resolved != receipt.get("command") or resolved not in files:
        raise ValueError("Pinned stdio command does not match the approved receipt")
    for name, expected in files.items():
        if not os.path.isabs(name) or not isinstance(expected, str) or len(expected) != 64:
            raise ValueError("Invalid pinned stdio artifact receipt")
        path = Path(name)
        if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError("Pinned stdio artifact changed: " + name)
    return True
