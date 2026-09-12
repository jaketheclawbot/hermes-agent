"""An accepted delivery must retry only its failed checkpoint acknowledgment."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from tests.gateway.test_background_process_notifications import (
    _FakeRegistry, _build_runner, _watcher_dict,
)


@pytest.mark.asyncio
async def test_accepted_delivery_retries_ack_without_reentering_delivery(monkeypatch, tmp_path):
    session = SimpleNamespace(output_buffer="done", exited=True, exit_code=0, command="true")
    registry = _FakeRegistry([session] * 3)
    registry.ensure_terminal_checkpoint = Mock(return_value=True)
    registry.acknowledge_terminal_notification = Mock(side_effect=[False, True])
    monkeypatch.setattr("tools.process_registry.process_registry", registry)
    runner = _build_runner(monkeypatch, tmp_path, "all")
    # The real enqueue contract returns None once the accepted lifecycle is
    # deduplicated. Re-enqueueing is not an acknowledgment retry.
    runner._enqueue_process_completion_notification = AsyncMock(side_effect=[True, None])
    watcher = _watcher_dict()
    watcher["notify_on_complete"] = True
    await runner._run_process_watcher(watcher)
    assert registry.acknowledge_terminal_notification.call_count == 2
    runner._enqueue_process_completion_notification.assert_awaited_once()
