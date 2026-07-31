"""Regression coverage for Discord sticky multi-participant thread routing."""
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from gateway.config import PlatformConfig
from plugins.platforms.discord import adapter as mod
from plugins.platforms.discord.adapter import DiscordAdapter, _apply_yaml_config


class DM: pass


class Thread:
    def __init__(self, ident=456, members=(), error=None):
        self.id, self.members, self.error = ident, list(members), error
        self.parent = self.parent_id = None
        self.name, self.topic = f"thread-{ident}", None
        self.guild = SimpleNamespace(id=1, name="guild", get_member=lambda _id: None)
    async def fetch_members(self):
        if self.error: raise self.error
        return self.members
    def history(self, **_kwargs):
        async def empty():
            if False: yield None
        return empty()


def message(thread, author_id, text, *, bot=False, reply_author=None, mentions=(), ident=None):
    author = SimpleNamespace(id=author_id, name=str(author_id), display_name=str(author_id), bot=bot)
    reference = None
    kind = mod.discord.MessageType.default
    if reply_author:
        target = SimpleNamespace(id=9000, author=reply_author, content="target", attachments=[])
        reference = SimpleNamespace(message_id=9000, resolved=target)
        kind = mod.discord.MessageType.reply
    return SimpleNamespace(id=ident or author_id, content=text, mentions=list(mentions), attachments=[],
        message_snapshots=[], reference=reference, created_at=datetime.now(timezone.utc),
        channel=thread, guild=thread.guild, author=author, type=kind)


@pytest.fixture
def adapter(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("DISCORD_ALLOW_ALL_USERS", "true")
    monkeypatch.setenv("DISCORD_ALLOW_BOTS", "all")
    monkeypatch.setenv("DISCORD_AUTO_THREAD", "false")
    monkeypatch.setattr(mod.discord, "DMChannel", DM)
    monkeypatch.setattr(mod.discord, "Thread", Thread)
    cfg = PlatformConfig(enabled=True, token="token")
    cfg.extra.update(require_mention=True, multi_human_thread_require_mention=True,
                     bots_require_inline_mention=True, history_backfill=False)
    obj = DiscordAdapter(cfg)
    obj._client = SimpleNamespace(user=SimpleNamespace(id=999, name="Jake", display_name="Jake", bot=True))
    obj._ready_event.set()
    obj._text_batch_delay_seconds = 0
    obj.handle_message = AsyncMock()
    return obj


@pytest.mark.asyncio
async def test_second_external_participant_stickily_gates(adapter):
    thread = Thread(members=[adapter._client.user, SimpleNamespace(id=41)])
    adapter._threads.mark("456")
    assert await adapter._dispatch_discord_message(message(thread, 41, "fluid", ident=1))
    adapter.handle_message.reset_mock()
    assert not await adapter._dispatch_discord_message(message(thread, 42, "ambient", ident=2))
    assert "456" in adapter._multi_human_threads
    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_addressing_and_bot_inline_rules(adapter):
    self_bot, other = adapter._client.user, SimpleNamespace(id=888, bot=True, name="other")
    thread = Thread(members=[self_bot, other])
    adapter._threads.mark("456"); adapter._multi_human_threads.add("456")
    assert await adapter._dispatch_discord_message(message(thread, 41, "reply", reply_author=self_bot, ident=3))
    assert not await adapter._dispatch_discord_message(message(thread, 41, "other", reply_author=other, ident=4))
    assert await adapter._dispatch_discord_message(message(thread, 41, "<@999> both", reply_author=other, mentions=[self_bot], ident=5))
    assert not await adapter._dispatch_discord_message(message(thread, 888, "reply chip", bot=True, reply_author=self_bot, mentions=[self_bot], ident=6))
    assert await adapter._dispatch_discord_message(message(thread, 888, "<@999> handoff", bot=True, reply_author=self_bot, mentions=[self_bot], ident=7))


@pytest.mark.asyncio
async def test_missing_baseline_reconciles_or_fails_closed(adapter, tmp_path):
    adapter._threads.mark("700")
    thread = Thread(700, [adapter._client.user, SimpleNamespace(id=41)])
    assert not await adapter._dispatch_discord_message(message(thread, 42, "second", ident=8))
    assert "700" in adapter._multi_human_threads
    adapter._threads.mark("701")
    broken = Thread(701, error=RuntimeError("unavailable"))
    assert not await adapter._dispatch_discord_message(message(broken, 41, "unknown", ident=9))
    assert "701" in DiscordAdapter(adapter.config)._multi_human_threads
    assert json.loads((tmp_path / "discord_thread_routing.json").read_text())["threads"]["701"]["mention_required"]


@pytest.mark.asyncio
async def test_join_reply_target_startup_and_first_engagement_contribute(adapter):
    bot, other = adapter._client.user, SimpleNamespace(id=888, bot=True)
    adapter._threads.mark("702"); adapter._observe_thread_human("702", "41")
    await adapter._handle_thread_member_join(SimpleNamespace(thread_id=702, id=42))
    assert "702" in adapter._multi_human_threads
    reply_thread = Thread(703, [bot]); adapter._threads.mark("703"); adapter._observe_thread_human("703", "41")
    assert not await adapter._dispatch_discord_message(message(reply_thread, 41, "elsewhere", reply_author=other, ident=10))
    assert "703" in adapter._multi_human_threads
    startup = Thread(704, [bot, SimpleNamespace(id=41), SimpleNamespace(id=42)])
    adapter._threads.mark("704"); startup.guild.threads = [startup]; adapter._client.guilds = [startup.guild]
    await adapter._reconcile_multi_human_threads(); assert "704" in adapter._multi_human_threads
    first = Thread(705, [bot, other, SimpleNamespace(id=41)])
    assert await adapter._dispatch_discord_message(message(first, 41, "<@999> engage", mentions=[bot], ident=11))
    assert "705" in adapter._multi_human_threads


def test_legacy_state_bounded_history_and_yaml_bridge(adapter, tmp_path):
    adapter._thread_first_human_ids["sticky"] = "1"; adapter._multi_human_threads.add("sticky")
    for index in range(600): adapter._thread_first_human_ids[f"fluid-{index}"] = str(index)
    adapter._save_multi_human_thread_routing()
    payload = json.loads((tmp_path / "discord_thread_routing.json").read_text())
    assert len(payload["threads"]) == 500 and payload["threads"]["sticky"]["mention_required"]
    with patch.dict("os.environ", {}, clear=False):
        import os
        os.environ.pop("DISCORD_MULTI_HUMAN_THREAD_REQUIRE_MENTION", None)
        _apply_yaml_config({}, {"multi_human_thread_require_mention": True})
        assert os.environ["DISCORD_MULTI_HUMAN_THREAD_REQUIRE_MENTION"] == "true"
