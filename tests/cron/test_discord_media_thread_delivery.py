"""Discord cron text and media must share the resolved delivery thread."""

import asyncio
import threading
from types import SimpleNamespace

import pytest

from cron.scheduler import _deliver_result
from gateway.config import GatewayConfig, Platform, PlatformConfig
from plugins.platforms.discord.adapter import DiscordAdapter


class _Channel:
    def __init__(self, channel_id):
        self.id = channel_id
        self.type = 0
        self.sends = []

    async def send(self, **kwargs):
        self.sends.append(kwargs)
        attachments = kwargs.get("files") or []
        return SimpleNamespace(
            id=1000 + len(self.sends),
            attachments=attachments,
        )


class _Client:
    def __init__(self, *channels):
        self.channels = {channel.id: channel for channel in channels}
        self.http = self
        self.requests = []

    def get_channel(self, channel_id):
        return self.channels.get(channel_id)

    async def fetch_channel(self, channel_id):
        return self.channels.get(channel_id)

    async def request(self, route, **kwargs):
        self.requests.append((route, kwargs))
        return {"id": "2001"}


@pytest.fixture
def discord_upload_primitives(monkeypatch):
    """Keep this test independent of gateway/conftest's discord module mock."""
    import plugins.platforms.discord.adapter as discord_platform

    class _File:
        def __init__(self, fp, filename=None, **_kwargs):
            self.fp = fp
            self.filename = filename

    class _Route:
        def __init__(self, method, path, **parameters):
            self.method = method
            self.url = path.format(**parameters)

    monkeypatch.setattr(discord_platform.discord, "File", _File)
    monkeypatch.setattr(discord_platform.discord.http, "Route", _Route)


@pytest.mark.parametrize(
    ("deliver", "origin", "expected_target", "suffix", "payload"),
    [
        (
            "origin",
            {
                "platform": "discord",
                "chat_id": "111",
                "thread_id": "222",
                "chat_type": "thread",
            },
            222,
            ".mp4",
            b"\x00\x00\x00\x18ftypmp42origin-video",
        ),
        ("discord:111:222", None, 222, ".mp4", b"explicit-video"),
        ("discord:111:222", None, 222, ".ogg", b"OggSencoded-audio"),
        ("discord:111:222", None, 222, ".png", b"\x89PNGencoded-image"),
        ("discord:111:222", None, 222, ".pdf", b"%PDF-encoded-document"),
        ("discord:111", None, 111, ".mp4", b"flat-video"),
    ],
)
def test_live_discord_cron_media_uses_resolved_text_target(
    monkeypatch,
    tmp_path,
    discord_upload_primitives,
    deliver,
    origin,
    expected_target,
    suffix,
    payload,
):
    """Exercise _deliver_result through DeliveryRouter and DiscordAdapter."""
    parent = _Channel(111)
    thread = _Channel(222)
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="test"))
    adapter._client = client = _Client(parent, thread)
    monkeypatch.setattr(adapter, "_is_forum_parent", lambda _channel: False)
    monkeypatch.setattr(adapter, "_record_discord_response", lambda **_kwargs: None)

    config = GatewayConfig(
        platforms={Platform.DISCORD: PlatformConfig(enabled=True, token="test")}
    )
    monkeypatch.setattr("gateway.config.load_gateway_config", lambda: config)
    monkeypatch.setattr(
        "cron.scheduler.load_config",
        lambda: {"cron": {"wrap_response": False, "mirror_delivery": False}},
    )

    media = tmp_path / f"report{suffix}"
    media.write_bytes(payload)
    job = {
        "id": "discord-thread-media",
        "name": "Discord thread media",
        "deliver": deliver,
    }
    if origin is not None:
        job["origin"] = origin

    loop = asyncio.new_event_loop()
    runner = threading.Thread(target=loop.run_forever, daemon=True)
    runner.start()
    try:
        error = _deliver_result(
            job,
            f"Report ready.\nMEDIA:{media}",
            adapters={Platform.DISCORD: adapter},
            loop=loop,
        )
    finally:
        loop.call_soon_threadsafe(loop.stop)
        runner.join(timeout=5)
        loop.close()

    assert error is None
    target = thread if expected_target == 222 else parent
    other = parent if expected_target == 222 else thread
    assert other.sends == []
    assert target.sends[0]["content"] == "Report ready."
    if suffix != ".ogg":
        assert [send.get("content") for send in target.sends] == [
            "Report ready.",
            None,
        ]
        assert len(target.sends[1]["files"]) == 1
        assert client.requests == []
    else:
        assert len(target.sends) == 1
        assert len(client.requests) == 1
        route, kwargs = client.requests[0]
        assert route.url == f"/channels/{expected_target}/messages"
        assert kwargs["form"][1]["value"] == payload
