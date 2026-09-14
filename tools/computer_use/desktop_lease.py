"""Cross-session exclusive lease for one shared interactive desktop.

Computer-use sessions have independent driver state but share the physical GUI.
This tiny file-backed coordinator prevents separate Hermes sessions/processes
from changing that GUI concurrently. Ownership lasts for one agent turn and is
released by ``AIAgent.run_conversation``'s finalizer.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator

_LEASE_SECONDS = 3600.0
_THREAD_GUARD = threading.RLock()


def _paths() -> tuple[Path, Path]:
    from hermes_constants import get_default_hermes_root

    root = get_default_hermes_root() / "runtime"
    return root / "desktop-lease.json", root / "desktop-lease.guard"


@contextmanager
def _guard(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    # flock semantics within one process differ across platforms; serialize
    # gateway worker threads explicitly, then use the file lock for processes.
    with _THREAD_GUARD:
        handle = open(path, "a+b")
        try:
            os.chmod(path, 0o600)
            if os.name == "nt":
                import msvcrt

                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            finally:
                handle.close()


def _read(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {"_invalid": True}
    except (OSError, ValueError, TypeError):
        return {"_invalid": True}


def _pid_alive(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, TypeError, ValueError):
        return False


def _write(path: Path, value: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    os.chmod(temp, 0o600)
    os.replace(temp, path)


def _label(session_id: str) -> str:
    try:
        from gateway.session_context import get_session_env

        return (
            get_session_env("HERMES_SESSION_CHAT_NAME", "").strip()
            or get_session_env("HERMES_SESSION_THREAD_ID", "").strip()
            or get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
            or session_id
        )[:160]
    except Exception:
        return session_id[:160]


def acquire_desktop(
    session_id: str,
    *,
    now: float | None = None,
    wait_seconds: float = 0,
) -> Dict[str, Any]:
    """Claim the desktop, optionally waiting in FIFO order for its release."""
    owner_id = str(session_id or "").strip() or f"pid:{os.getpid()}"
    state_path, guard_path = _paths()
    deadline = time.monotonic() + max(0.0, float(wait_seconds))
    while True:
        stamp = time.time() if now is None else float(now)
        with _guard(guard_path):
            state = _read(state_path)
            if state.get("_invalid"):
                return {
                    "ok": False,
                    "code": "desktop_coordinator_unavailable",
                    "error": "Shared desktop lease state is unreadable; refusing UI input.",
                    "hint": "Repair the lease state before retrying; do not bypass it.",
                }
            owner = state.get("owner") if isinstance(state.get("owner"), dict) else None
            if owner and (
                float(owner.get("expires_at") or 0) <= stamp
                or not _pid_alive(owner.get("pid"))
            ):
                owner = None
            queue = [
                item for item in state.get("queue", [])
                if isinstance(item, dict)
                and stamp - float(item.get("queued_at") or 0) < _LEASE_SECONDS
            ]
            blocked = bool(
                (owner and owner.get("session_id") != owner_id)
                or (not owner and queue and queue[0].get("session_id") != owner_id)
            )
            if not blocked:
                queue = [item for item in queue if item.get("session_id") != owner_id]
                owner = {
                    "session_id": owner_id,
                    "label": _label(owner_id),
                    "pid": os.getpid(),
                    "acquired_at": (owner or {}).get("acquired_at", stamp),
                    "expires_at": stamp + _LEASE_SECONDS,
                }
                _write(state_path, {"owner": owner, "queue": queue})
                return {"ok": True, "owner": owner_id}
            if not any(item.get("session_id") == owner_id for item in queue):
                queue.append({
                    "session_id": owner_id,
                    "label": _label(owner_id),
                    "pid": os.getpid(),
                    "queued_at": stamp,
                })
            _write(state_path, {"owner": owner, "queue": queue})
            position = next(
                i for i, item in enumerate(queue, 1)
                if item.get("session_id") == owner_id
            )
            busy = {
                "ok": False,
                "code": "desktop_busy",
                "error": "Shared desktop is owned or reserved by another Hermes session.",
                "owner": (owner or queue[0]).get("label") or (owner or queue[0]).get("session_id"),
                "queue_position": position,
                "hint": (
                    "No desktop action was executed. Continue non-desktop work; "
                    "retry computer_use later after the owning turn finishes."
                ),
            }
        if time.monotonic() >= deadline:
            return busy
        time.sleep(0.2)


def release_desktop(session_id: str) -> bool:
    """Release caller ownership and remove its waiting-row on turn exit."""
    owner_id = str(session_id or "").strip() or f"pid:{os.getpid()}"
    state_path, guard_path = _paths()
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return False
        owner = state.get("owner") if isinstance(state.get("owner"), dict) else None
        queue = [
            item for item in state.get("queue", [])
            if isinstance(item, dict) and item.get("session_id") != owner_id
        ]
        owned = bool(owner and owner.get("session_id") == owner_id)
        if not owned and len(queue) == len(state.get("queue", [])):
            return False
        _write(state_path, {"owner": None if owned else owner, "queue": queue})
        return owned


_OSASCRIPT = re.compile(r"(?<![\w-])(?:[^\s'\"]*/)?osascript(?![\w-])", re.I)


def command_uses_desktop_automation(command: str) -> bool:
    """Recognize direct AppleScript UI entrypoints that must share the lease."""
    return bool(_OSASCRIPT.search(str(command or "")))
