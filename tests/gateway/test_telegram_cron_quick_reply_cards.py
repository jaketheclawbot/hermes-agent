import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter


@pytest.fixture
def adapter(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    obj = TelegramAdapter(PlatformConfig(enabled=True, token="token"))
    obj._bot = SimpleNamespace(send_message=AsyncMock(return_value=SimpleNamespace(message_id=88)))
    return obj


def test_card_opt_out_persistence_expiry_and_pruning(adapter, monkeypatch):
    card = {"job_id": "job-1", "name": "Digest", "output": "all good"}
    assert adapter._cron_reply_markup({"cron_quick_reply_card": card}) is not None
    adapter._store_cron_reply_context("7", "8", card)
    assert "all good" in adapter._lookup_cron_reply_context("7", "8")
    state = adapter._cron_reply_state_load()
    state["old:1"] = {"expires_at": 1, "context": "old"}
    adapter._cron_reply_state_save(state)
    assert "old:1" not in adapter._cron_reply_state_prune(adapter._cron_reply_state_load())
    adapter._cron_quick_reply_cards_enabled = False
    assert adapter._cron_reply_markup({"cron_quick_reply_card": card}) is None


@pytest.mark.asyncio
async def test_callback_authorization_and_helper_context(adapter, monkeypatch):
    adapter._store_cron_reply_context("7", "8", {"job_id": "j", "output": "status"})
    query = SimpleNamespace(
        data="cj:reply", from_user=SimpleNamespace(id=41, first_name="A"),
        message=SimpleNamespace(chat_id=7, message_id=8, message_thread_id=None,
                                chat=SimpleNamespace(type="private")),
        answer=AsyncMock(),
    )
    adapter._is_callback_user_authorized = lambda *args, **kwargs: False
    await adapter._handle_callback_query(SimpleNamespace(callback_query=query), None)
    adapter._bot.send_message.assert_not_awaited()
    adapter._is_callback_user_authorized = lambda *args, **kwargs: True
    await adapter._handle_callback_query(SimpleNamespace(callback_query=query), None)
    adapter._bot.send_message.assert_awaited_once()
    assert "status" in adapter._lookup_cron_reply_context(7, 88)


def test_rich_path_is_bypassed_for_cards(adapter):
    adapter._rich_eligible = lambda _content: True
    assert not adapter._should_attempt_rich("hello", {"cron_quick_reply_card": {}})
