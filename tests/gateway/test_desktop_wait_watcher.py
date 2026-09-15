from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.run import GatewayRunner
from tools.computer_use import desktop_lease


@pytest.mark.asyncio
async def test_desktop_wait_watcher_wakes_original_session(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._running = True
    event = {
        "type": "desktop_wait_ready",
        "wait_id": "desktop_wait_1",
        "claim_id": "desktop_claim_1",
        "platform": "discord",
        "chat_id": "thread-1",
        "chat_type": "thread",
        "thread_id": "thread-1",
        "session_key": "agent:main:discord:thread:thread-1:thread-1",
        "parent_session_id": "session-1",
    }
    claims = [[event], []]
    completed = []

    def claim(_consumer):
        return claims.pop(0)

    def complete(wait_id, claim_id, *, delivered):
        completed.append((wait_id, claim_id, delivered))
        runner._running = False
        return True

    monkeypatch.setattr(desktop_lease, "claim_desktop_wait_events", claim)
    monkeypatch.setattr(desktop_lease, "complete_desktop_wait_event", complete)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._inject_watch_notification = AsyncMock(return_value=True)

    await runner._desktop_wait_watcher(interval=0)

    runner._inject_watch_notification.assert_awaited_once()
    prompt = runner._inject_watch_notification.await_args.args[0]
    assert "shared desktop is now available" in prompt
    assert "retrying the previously blocked desktop action" in prompt
    assert completed == [("desktop_wait_1", "desktop_claim_1", True)]


@pytest.mark.asyncio
async def test_desktop_wait_expiry_sends_direct_notice_to_original_thread(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._running = True
    event = {
        "type": "desktop_wait_expired",
        "wait_id": "desktop_wait_2",
        "claim_id": "desktop_claim_2",
        "platform": "discord",
        "chat_id": "thread-2",
        "chat_type": "thread",
        "thread_id": "thread-2",
        "session_key": "agent:main:discord:thread:thread-2:thread-2",
        "parent_session_id": "session-2",
    }
    source = SimpleNamespace(chat_id="thread-2")
    adapter = SimpleNamespace(send=AsyncMock(return_value=SimpleNamespace(success=True)))
    completed = []

    def complete(wait_id, claim_id, *, delivered):
        completed.append((wait_id, claim_id, delivered))
        runner._running = False
        return True

    monkeypatch.setattr(desktop_lease, "claim_desktop_wait_events", lambda _consumer: [event])
    monkeypatch.setattr(desktop_lease, "complete_desktop_wait_event", complete)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._build_process_event_source = lambda _event: source
    runner._adapter_for_source = lambda _source: adapter
    runner._thread_metadata_for_source = lambda _source: {"thread_id": "thread-2"}

    await runner._desktop_wait_watcher(interval=0)

    adapter.send.assert_awaited_once()
    args = adapter.send.await_args
    assert args.args[0] == "thread-2"
    assert "expired after waiting three hours" in args.args[1]
    assert args.kwargs["metadata"] == {"thread_id": "thread-2"}
    assert completed == [("desktop_wait_2", "desktop_claim_2", True)]


@pytest.mark.asyncio
async def test_desktop_wait_delivery_failure_keeps_event_retryable(monkeypatch):
    runner = object.__new__(GatewayRunner)
    runner._running = True
    event = {
        "type": "desktop_wait_ready",
        "wait_id": "desktop_wait_3",
        "claim_id": "desktop_claim_3",
    }
    completed = []

    def complete(wait_id, claim_id, *, delivered):
        completed.append((wait_id, claim_id, delivered))
        runner._running = False
        return True

    monkeypatch.setattr(desktop_lease, "claim_desktop_wait_events", lambda _consumer: [event])
    monkeypatch.setattr(desktop_lease, "complete_desktop_wait_event", complete)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._inject_watch_notification = AsyncMock(return_value=False)

    await runner._desktop_wait_watcher(interval=0)

    assert completed == [("desktop_wait_3", "desktop_claim_3", False)]
