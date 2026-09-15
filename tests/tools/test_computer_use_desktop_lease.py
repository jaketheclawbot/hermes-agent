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


def test_parked_wait_is_durable_for_three_hours_and_preserved_on_turn_exit(
    lease_home, monkeypatch
):
    desktop_lease.acquire_desktop("owner", now=100)
    monkeypatch.setattr(
        desktop_lease,
        "_routing",
        lambda sid: {
            "platform": "discord",
            "chat_id": "thread-1",
            "chat_type": "thread",
            "thread_id": "thread-1",
            "session_key": "agent:main:discord:thread:thread-1:thread-1",
            "parent_session_id": sid,
        },
    )

    parked = desktop_lease.acquire_desktop("waiter", now=101, park=True)

    assert parked["code"] == "desktop_parked"
    assert parked["expires_in_seconds"] == 10800
    assert parked["queue_position"] == 1
    desktop_lease.release_desktop("waiter")
    state = json.loads(lease_home.read_text())
    assert state["queue"][0]["session_id"] == "waiter"
    assert state["queue"][0]["expires_at"] == 101 + 10800
    assert state["queue"][0]["thread_id"] == "thread-1"


def test_ready_event_is_claimed_once_and_waiter_acquires_after_wake(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    parked = desktop_lease.acquire_desktop("waiter", now=101, park=True)
    desktop_lease.release_desktop("owner")

    events = desktop_lease.claim_desktop_wait_events("gateway-a", now=102)
    assert len(events) == 1
    event = events[0]
    assert event["type"] == "desktop_wait_ready"
    assert event["kind"] == "ready"
    assert event["wait_id"] == parked["wait_id"]
    assert desktop_lease.claim_desktop_wait_events("gateway-b", now=102) == []

    assert desktop_lease.complete_desktop_wait_event(
        event["wait_id"], event["claim_id"], delivered=True, now=103
    )
    assert desktop_lease.acquire_desktop("waiter", now=104)["ok"] is True
    assert json.loads(lease_home.read_text())["queue"] == []


def test_failed_ready_delivery_releases_claim_for_retry(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    desktop_lease.acquire_desktop("waiter", now=101, park=True)
    desktop_lease.release_desktop("owner")
    event = desktop_lease.claim_desktop_wait_events("gateway-a", now=102)[0]

    assert desktop_lease.complete_desktop_wait_event(
        event["wait_id"], event["claim_id"], delivered=False, now=103
    )
    retried = desktop_lease.claim_desktop_wait_events("gateway-b", now=104)
    assert len(retried) == 1
    assert retried[0]["wait_id"] == event["wait_id"]
    assert retried[0]["claim_id"] != event["claim_id"]


def test_expiry_event_is_retained_until_delivery_then_removed(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    parked = desktop_lease.acquire_desktop("waiter", now=101, park=True)

    event = desktop_lease.claim_desktop_wait_events(
        "gateway-a", now=101 + desktop_lease._WAIT_SECONDS + 1
    )[0]
    assert event["type"] == "desktop_wait_expired"
    assert json.loads(lease_home.read_text())["queue"]

    assert desktop_lease.complete_desktop_wait_event(
        parked["wait_id"], event["claim_id"], delivered=True, now=20000
    )
    assert json.loads(lease_home.read_text())["queue"] == []


def test_ready_wake_that_never_acquires_still_emits_expiry(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    parked = desktop_lease.acquire_desktop("waiter", now=101, park=True)
    desktop_lease.release_desktop("owner")
    ready = desktop_lease.claim_desktop_wait_events("gateway-a", now=102)[0]
    desktop_lease.complete_desktop_wait_event(
        parked["wait_id"], ready["claim_id"], delivered=True, now=103
    )

    expired = desktop_lease.claim_desktop_wait_events(
        "gateway-b", now=101 + desktop_lease._WAIT_SECONDS + 1
    )
    assert len(expired) == 1
    assert expired[0]["type"] == "desktop_wait_expired"


def test_expired_parked_wait_is_not_pruned_before_notice_delivery(lease_home):
    desktop_lease.acquire_desktop("owner", now=100)
    desktop_lease.acquire_desktop("waiter", now=101, park=True)
    desktop_lease.release_desktop("owner")

    later = 101 + desktop_lease._WAIT_SECONDS + 1
    blocked = desktop_lease.acquire_desktop("newcomer", now=later)
    assert blocked["code"] == "desktop_busy"
    state = json.loads(lease_home.read_text())
    assert [item["session_id"] for item in state["queue"]] == ["waiter", "newcomer"]


def test_release_wakes_local_gateway_monitor(lease_home):
    signals = []
    desktop_lease.set_desktop_wait_notifier(lambda: signals.append("wake"))
    try:
        desktop_lease.acquire_desktop("owner", now=100)
        desktop_lease.acquire_desktop("waiter", now=101, park=True)
        signals.clear()
        desktop_lease.release_desktop("owner")
        assert signals == ["wake"]
    finally:
        desktop_lease.set_desktop_wait_notifier(None)


def test_new_user_turn_cancels_parked_wait(lease_home, monkeypatch):
    monkeypatch.setattr(
        desktop_lease,
        "_routing",
        lambda session_id: {
            "session_key": "discord:thread:queued",
            "parent_session_id": session_id,
            "origin_session_id": session_id,
        },
    )
    desktop_lease.acquire_desktop("owner", now=100)
    desktop_lease.acquire_desktop("waiter", now=101, park=True)

    assert desktop_lease.cancel_desktop_wait("discord:thread:queued") is True
    assert desktop_lease.cancel_desktop_wait("waiter") is False
    assert json.loads(lease_home.read_text())["queue"] == []


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
