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
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Dict, Iterator

_LEASE_SECONDS = 3600.0
_WAIT_SECONDS = 3 * 60 * 60.0
_EVENT_CLAIM_SECONDS = 30.0
_THREAD_GUARD = threading.RLock()
_WAIT_NOTIFIER: Callable[[], None] | None = None


def set_desktop_wait_notifier(callback: Callable[[], None] | None) -> None:
    """Register the local gateway's release-triggered wake callback."""
    global _WAIT_NOTIFIER
    _WAIT_NOTIFIER = callback


def _notify_waiter_monitor() -> None:
    callback = _WAIT_NOTIFIER
    if callback is None:
        return
    try:
        callback()
    except Exception:
        pass


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


def _routing(session_id: str) -> Dict[str, str]:
    """Capture a durable return address for a parked gateway turn."""
    try:
        from gateway.session_context import get_session_env
        from hermes_constants import get_hermes_home

        values = {
            "platform": get_session_env("HERMES_SESSION_PLATFORM", ""),
            "chat_id": get_session_env("HERMES_SESSION_CHAT_ID", ""),
            "chat_type": get_session_env("HERMES_SESSION_CHAT_TYPE", ""),
            "thread_id": get_session_env("HERMES_SESSION_THREAD_ID", ""),
            "user_id": get_session_env("HERMES_SESSION_USER_ID", ""),
            "user_name": get_session_env("HERMES_SESSION_USER_NAME", ""),
            "scope_id": get_session_env("HERMES_SESSION_SCOPE_ID", ""),
            "session_key": get_session_env("HERMES_SESSION_KEY", ""),
            "message_id": get_session_env("HERMES_SESSION_MESSAGE_ID", ""),
            "origin_profile": get_session_env("HERMES_SESSION_PROFILE", ""),
            "origin_hermes_home": str(get_hermes_home()),
            "parent_session_id": session_id,
            "origin_session_id": session_id,
        }
        return {key: str(value or "").strip() for key, value in values.items()}
    except Exception:
        return {"parent_session_id": session_id, "origin_session_id": session_id}



def acquire_desktop(
    session_id: str,
    *,
    now: float | None = None,
    wait_seconds: float = 0,
    park: bool = False,
) -> Dict[str, Any]:
    """Claim the desktop, block briefly, or durably park in FIFO order."""
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
                and (
                    item.get("parked")
                    or float(item.get("expires_at") or (
                        float(item.get("queued_at") or 0) + _LEASE_SECONDS
                    )) > stamp
                )
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

            existing = next(
                (item for item in queue if item.get("session_id") == owner_id),
                None,
            )
            if existing is None:
                existing = {
                    "session_id": owner_id,
                    "label": _label(owner_id),
                    "pid": os.getpid(),
                    "queued_at": stamp,
                    "expires_at": stamp + (_WAIT_SECONDS if park else _LEASE_SECONDS),
                }
                if park:
                    existing.update({
                        "wait_id": f"desktop_wait_{uuid.uuid4().hex}",
                        "parked": True,
                        **_routing(owner_id),
                    })
                queue.append(existing)
            elif park and not existing.get("parked"):
                existing.update({
                    "wait_id": f"desktop_wait_{uuid.uuid4().hex}",
                    "parked": True,
                    "expires_at": stamp + _WAIT_SECONDS,
                    **_routing(owner_id),
                })
            _write(state_path, {"owner": owner, "queue": queue})
            if park:
                _notify_waiter_monitor()
            position = next(
                i for i, item in enumerate(queue, 1)
                if item.get("session_id") == owner_id
            )
            blocker = owner or queue[0]
            if park:
                return {
                    "ok": False,
                    "code": "desktop_parked",
                    "error": "Shared desktop is currently reserved by another Hermes session.",
                    "owner": blocker.get("label") or blocker.get("session_id"),
                    "queue_position": position,
                    "wait_id": existing.get("wait_id"),
                    "expires_in_seconds": int(_WAIT_SECONDS),
                    "hint": (
                        "No desktop action was executed. This request is durably parked for up "
                        "to three hours and will wake this session automatically when the "
                        "desktop is available. Do not poll or retry during this turn."
                    ),
                }
            busy = {
                "ok": False,
                "code": "desktop_busy",
                "error": "Shared desktop is owned or reserved by another Hermes session.",
                "owner": blocker.get("label") or blocker.get("session_id"),
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
    """Release ownership; retain durable parked rows for the gateway watcher."""
    owner_id = str(session_id or "").strip() or f"pid:{os.getpid()}"
    state_path, guard_path = _paths()
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return False
        owner = state.get("owner") if isinstance(state.get("owner"), dict) else None
        original_queue = [item for item in state.get("queue", []) if isinstance(item, dict)]
        queue = [
            item for item in original_queue
            if item.get("session_id") != owner_id or item.get("parked")
        ]
        owned = bool(owner and owner.get("session_id") == owner_id)
        if not owned and len(queue) == len(original_queue):
            return False
        _write(state_path, {"owner": None if owned else owner, "queue": queue})
        if owned:
            _notify_waiter_monitor()
        return owned


def cancel_desktop_wait(session_ref: str) -> bool:
    """Cancel a parked request when a newer real user turn supersedes it."""
    ref = str(session_ref or "").strip()
    if not ref:
        return False
    state_path, guard_path = _paths()
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return False
        queue = [item for item in state.get("queue", []) if isinstance(item, dict)]
        kept = [
            item for item in queue
            if not (
                item.get("parked")
                and (item.get("session_id") == ref or item.get("session_key") == ref)
            )
        ]
        if len(kept) == len(queue):
            return False
        _write(state_path, {"owner": state.get("owner"), "queue": kept})
        return True


def discard_desktop_wait(wait_id: str) -> bool:
    """Remove one exact stale wait without affecting a newer request."""
    target_id = str(wait_id or "").strip()
    if not target_id:
        return False
    state_path, guard_path = _paths()
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return False
        queue = [item for item in state.get("queue", []) if isinstance(item, dict)]
        kept = [item for item in queue if item.get("wait_id") != target_id]
        if len(kept) == len(queue):
            return False
        _write(state_path, {"owner": state.get("owner"), "queue": kept})
        return True


def claim_desktop_wait_events(
    consumer_id: str,
    *,
    now: float | None = None,
    limit: int = 20,
) -> list[Dict[str, Any]]:
    """Atomically claim ready/expired parked-wait events for gateway delivery."""
    stamp = time.time() if now is None else float(now)
    state_path, guard_path = _paths()
    claimed: list[Dict[str, Any]] = []
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return []
        owner = state.get("owner") if isinstance(state.get("owner"), dict) else None
        if owner and (
            float(owner.get("expires_at") or 0) <= stamp
            or not _pid_alive(owner.get("pid"))
        ):
            owner = None
        queue = [item for item in state.get("queue", []) if isinstance(item, dict)]
        for index, item in enumerate(queue):
            if not item.get("parked"):
                continue
            expires_at = float(item.get("expires_at") or 0)
            kind = "expired" if expires_at <= stamp else None
            if kind is None:
                if item.get("event_delivered_at"):
                    continue
                if owner is None and index == 0:
                    kind = "ready"
            elif (
                item.get("event_kind") == "ready"
                and item.get("event_delivered_at")
            ):
                # A successful wake that never acquired the desktop still owes
                # the user the explicit three-hour expiry notice.
                item.pop("event_delivered_at", None)
                item.pop("event_claim_id", None)
                item.pop("event_claim_owner", None)
                item.pop("event_claim_until", None)
            elif item.get("event_delivered_at"):
                continue
            if kind is None:
                continue
            claim_until = float(item.get("event_claim_until") or 0)
            if claim_until > stamp:
                continue
            claim_id = f"desktop_claim_{uuid.uuid4().hex}"
            item["event_kind"] = kind
            item["event_claim_id"] = claim_id
            item["event_claim_owner"] = str(consumer_id or "")
            item["event_claim_until"] = stamp + _EVENT_CLAIM_SECONDS
            event = dict(item)
            event.update({
                "type": f"desktop_wait_{kind}",
                "kind": kind,
                "claim_id": claim_id,
            })
            claimed.append(event)
            if len(claimed) >= max(1, int(limit)):
                break
        _write(state_path, {"owner": owner, "queue": queue})
    return claimed


def complete_desktop_wait_event(
    wait_id: str,
    claim_id: str,
    *,
    delivered: bool,
    now: float | None = None,
) -> bool:
    """Acknowledge one claimed event; expired rows leave only after delivery."""
    stamp = time.time() if now is None else float(now)
    state_path, guard_path = _paths()
    with _guard(guard_path):
        state = _read(state_path)
        if state.get("_invalid"):
            return False
        queue = [item for item in state.get("queue", []) if isinstance(item, dict)]
        target = next((item for item in queue if item.get("wait_id") == wait_id), None)
        if target is None or target.get("event_claim_id") != claim_id:
            return False
        if not delivered:
            target.pop("event_claim_id", None)
            target.pop("event_claim_owner", None)
            target.pop("event_claim_until", None)
            _write(state_path, {"owner": state.get("owner"), "queue": queue})
            return True
        if target.get("event_kind") == "expired":
            queue = [item for item in queue if item.get("wait_id") != wait_id]
        else:
            target["event_delivered_at"] = stamp
            target.pop("event_claim_id", None)
            target.pop("event_claim_owner", None)
            target.pop("event_claim_until", None)
        _write(state_path, {"owner": state.get("owner"), "queue": queue})
        return True


_OSASCRIPT = re.compile(r"(?<![\w-])(?:[^\s'\"]*/)?osascript(?![\w-])", re.I)


def command_uses_desktop_automation(command: str) -> bool:
    """Recognize direct AppleScript UI entrypoints that must share the lease."""
    return bool(_OSASCRIPT.search(str(command or "")))
