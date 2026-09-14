import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from tools.computer_use import desktop_lease


@pytest.fixture
def lease_home(tmp_path, monkeypatch):
    state = tmp_path / "desktop-lease.json"
    guard = tmp_path / "desktop-lease.guard"
    monkeypatch.setattr(desktop_lease, "_paths", lambda: (state, guard))
    return state


def test_second_session_is_queued_and_cannot_take_desktop(lease_home):
    assert desktop_lease.acquire_desktop("owner", now=100)["ok"] is True

    busy = desktop_lease.acquire_desktop("waiter", now=101)

    assert busy == {
        "ok": False,
        "code": "desktop_busy",
        "error": "Shared desktop is owned or reserved by another Hermes session.",
        "owner": "owner",
        "queue_position": 1,
        "hint": (
            "No desktop action was executed. Continue non-desktop work; "
            "retry computer_use later after the owning turn finishes."
        ),
    }
    state = json.loads(lease_home.read_text())
    assert [item["session_id"] for item in state["queue"]] == ["waiter"]


def test_simultaneous_gateway_threads_cannot_both_acquire(lease_home):
    barrier = Barrier(2)

    def claim(session_id):
        barrier.wait()
        return desktop_lease.acquire_desktop(session_id, now=100)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ("a", "b")))

    assert sum(result["ok"] for result in results) == 1
    assert {result.get("code") for result in results if not result["ok"]} == {
        "desktop_busy"
    }


def test_waiting_session_runs_automatically_after_owner_releases(lease_home):
    desktop_lease.acquire_desktop("owner")
    with ThreadPoolExecutor(max_workers=1) as pool:
        waiting = pool.submit(
            desktop_lease.acquire_desktop, "waiter", wait_seconds=2
        )
        time.sleep(0.3)
        assert not waiting.done()
        desktop_lease.release_desktop("owner")
        assert waiting.result(timeout=2)["ok"] is True


def test_owner_is_reentrant_then_release_allows_waiter(lease_home):
    first = desktop_lease.acquire_desktop("owner", now=100)
    refreshed = desktop_lease.acquire_desktop("owner", now=200)
    assert first["ok"] and refreshed["ok"]
    assert json.loads(lease_home.read_text())["owner"]["expires_at"] == 200 + desktop_lease._LEASE_SECONDS

    assert desktop_lease.release_desktop("other") is False
    assert desktop_lease.release_desktop("owner") is True
    assert desktop_lease.acquire_desktop("waiter", now=201)["ok"] is True
    state = json.loads(lease_home.read_text())
    assert state["owner"]["session_id"] == "waiter"
    assert state["queue"] == []


def test_turn_exit_removes_non_owner_from_visible_queue(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    desktop_lease.acquire_desktop("waiter", now=101)

    assert desktop_lease.release_desktop("waiter") is False
    state = json.loads(lease_home.read_text())
    assert state["owner"]["session_id"] == "owner"
    assert state["queue"] == []


def test_expired_or_dead_owner_recovers_without_manual_cleanup(lease_home):
    desktop_lease.acquire_desktop("expired", now=0)
    result = desktop_lease.acquire_desktop("live", now=desktop_lease._LEASE_SECONDS + 1)
    assert result["ok"] is True

    state = json.loads(lease_home.read_text())
    state["owner"] = {
        "session_id": "dead",
        "label": "dead",
        "pid": 99999999,
        "acquired_at": 5000,
        "expires_at": 999999,
    }
    lease_home.write_text(json.dumps(state))
    result = desktop_lease.acquire_desktop("next", now=5001)
    assert result["ok"] is True
    assert json.loads(lease_home.read_text())["owner"]["session_id"] == "next"


def test_corrupt_state_fails_closed(lease_home):
    lease_home.write_text("not json")
    result = desktop_lease.acquire_desktop("me", now=1)
    assert result["ok"] is False
    assert result["code"] == "desktop_coordinator_unavailable"


def test_direct_osascript_detection_is_narrow():
    assert desktop_lease.command_uses_desktop_automation(
        "osascript -e 'tell application \"System Events\" to keystroke \"g\"'"
    )
    assert desktop_lease.command_uses_desktop_automation(
        "/usr/bin/osascript script.scpt"
    )
    assert desktop_lease.command_uses_desktop_automation(
        "python -c \"subprocess.run(['osascript', '-e', 'return 1'])\""
    )
    assert not desktop_lease.command_uses_desktop_automation(
        "python -m pytest tests/test_osascript_docs.py"
    )


def test_computer_use_busy_fails_before_backend(monkeypatch):
    from tools.computer_use import tool

    monkeypatch.setattr(
        desktop_lease,
        "acquire_desktop",
        lambda _sid, **_kw: {"ok": False, "code": "desktop_busy", "owner": "another"},
    )
    monkeypatch.setattr(tool, "_get_backend", lambda **_kw: pytest.fail("backend started"))

    result = json.loads(tool.handle_computer_use({"action": "capture"}, session_id="me"))
    assert result["code"] == "desktop_busy"


def test_terminal_osascript_busy_never_executes(monkeypatch):
    from tools import terminal_tool

    monkeypatch.setattr(
        desktop_lease,
        "acquire_desktop",
        lambda _sid, **_kw: {"ok": False, "code": "desktop_busy", "owner": "another"},
    )
    monkeypatch.setattr(terminal_tool, "terminal_tool", lambda **_kw: pytest.fail("executed"))

    result = json.loads(terminal_tool._handle_terminal(
        {"command": "osascript -e 'return 1'"}, session_id="me"
    ))
    assert result["code"] == "desktop_busy"


def test_background_osascript_is_refused(monkeypatch):
    from tools import terminal_tool

    monkeypatch.setattr(terminal_tool, "terminal_tool", lambda **_kw: pytest.fail("executed"))
    result = json.loads(terminal_tool._handle_terminal(
        {"command": "osascript -e 'return 1'", "background": True},
        session_id="me",
    ))
    assert "cannot run in the background" in result["error"]


def test_execute_code_osascript_busy_never_executes(monkeypatch):
    from tools import code_execution_tool

    monkeypatch.setattr(
        desktop_lease,
        "acquire_desktop",
        lambda _sid, **_kw: {"ok": False, "code": "desktop_busy", "owner": "another"},
    )
    monkeypatch.setattr(code_execution_tool, "execute_code", lambda **_kw: pytest.fail("executed"))

    result = json.loads(code_execution_tool._execute_code_handler(
        {"code": "import subprocess; subprocess.run(['osascript', '-e', 'return 1'])"},
        session_id="me",
    ))
    assert result["code"] == "desktop_busy"
