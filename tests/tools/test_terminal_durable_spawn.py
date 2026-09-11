"""Effective notification routing must precede spawn and checkpoint publication."""
import json
from types import SimpleNamespace

import pytest
from tests.tools.test_notify_on_complete import _silent_bg_harness


@pytest.mark.parametrize("supported", [False, True])
def test_notification_contract_is_resolved_before_spawn(monkeypatch, tmp_path, supported):
    from tools.process_registry import process_registry
    import gateway.session_context as context

    tt = _silent_bg_harness(monkeypatch, tmp_path)
    monkeypatch.setattr(context, "async_delivery_supported", lambda: supported)
    origin = {"PLATFORM": "discord", "CHAT_ID": "chat", "THREAD_ID": "thread", "ID": "parent"}
    monkeypatch.setattr(context, "get_session_env", lambda key, default="": origin.get(key.removeprefix("HERMES_SESSION_"), default))
    calls = []

    def spawn(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(id="proc_contract", pid=123, notify_on_complete=kwargs["notify_on_complete"],
                               watcher_platform="")

    monkeypatch.setattr(process_registry, "spawn_local", spawn)
    result = json.loads(tt.terminal_tool(command="true", background=True, notify_on_complete=True))
    assert not result.get("error"), result
    assert calls[0]["notify_on_complete"] is supported
    assert result["notify_on_complete"] is supported
    if supported:
        assert calls[0]["notification_origin"]["watcher_thread_id"] == "thread"
        assert calls[0]["notification_origin"]["parent_session_id"] == "parent"
    else:
        assert calls[0]["notification_origin"] == {}


@pytest.mark.parametrize("pty", [False, True])
def test_reader_starts_after_routed_checkpoint(monkeypatch, tmp_path, pty):
    import tools.process_registry as module
    checkpoint = tmp_path / "processes.json"
    monkeypatch.setattr(module, "CHECKPOINT_PATH", checkpoint)
    # Exercise the real pipe/PTY reader and checkpoint ordering without
    # depending on the developer's potentially expensive login-shell startup.
    monkeypatch.setattr(module, "_find_shell", lambda: "/bin/sh")
    registry = module.ProcessRegistry()
    observed = []

    def reader(session):
        rows = json.loads(checkpoint.read_text())
        row = next(r for r in rows if r["session_id"] == session.id)
        observed.append((row["watcher_thread_id"], row["parent_session_id"], session.id in registry._running))
        # Run the genuine reader/finish path after inspecting its first checkpoint.
        original(session)

    method = "_pty_reader_loop" if pty else "_reader_loop"
    original = getattr(registry, method)
    monkeypatch.setattr(registry, method, reader)
    session = registry.spawn_local(command="printf durable", cwd=str(tmp_path), use_pty=pty,
        notify_on_complete=True, notification_origin={"watcher_platform": "discord", "watcher_thread_id": "thread", "parent_session_id": "parent"})
    session._reader_thread.join(timeout=20)
    assert not session._reader_thread.is_alive()
    assert observed == [("thread", "parent", True)]
    assert registry.completion_queue.get_nowait()["session_id"] == session.id
    rows = json.loads(checkpoint.read_text())
    assert rows[0]["terminal_event"]["session_id"] == session.id
    assert rows[0]["watcher_thread_id"] == "thread"


def test_cwd_error_releases_notify_reservation(monkeypatch, tmp_path):
    from tools.process_registry import process_registry
    import gateway.session_context as context
    tt = _silent_bg_harness(monkeypatch, tmp_path)
    monkeypatch.setattr(context, "async_delivery_supported", lambda: True)
    before = process_registry._notify_spawn_reservations
    monkeypatch.setattr(tt, "_resolve_command_cwd", lambda **kw: (_ for _ in ()).throw(ValueError("bad cwd")))
    result = json.loads(tt.terminal_tool(command="true", background=True, notify_on_complete=True))
    assert result.get("error")
    assert process_registry._notify_spawn_reservations == before


@pytest.mark.parametrize("write_ok", [False, True])
def test_sandbox_poller_starts_only_after_checkpoint(monkeypatch, tmp_path, write_ok):
    from unittest.mock import Mock
    import tools.process_registry as module
    registry = module.ProcessRegistry()
    monkeypatch.setattr(module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(registry, "_env_temp_dir", lambda _: str(tmp_path))
    env = SimpleNamespace(execute=Mock(return_value={"output": "12345", "returncode": 0}))
    seen = []
    def poller(session, *args):
        rows = json.loads((tmp_path / "processes.json").read_text())
        seen.append(rows[0]["session_id"] == session.id and session.id in registry._running)
    monkeypatch.setattr(registry, "_env_poller_loop", poller)
    if not write_ok:
        monkeypatch.setattr(registry, "_write_checkpoint", lambda: False)
        with pytest.raises(module.CheckpointPersistenceError):
            registry.spawn_via_env(env, "true", str(tmp_path), notify_on_complete=True)
        assert not registry._running
        assert seen == []
        assert env.execute.call_args.args[0] == "kill 12345 2>/dev/null"
    else:
        session = registry.spawn_via_env(env, "true", str(tmp_path), notify_on_complete=True)
        session._reader_thread.join(5)
        assert seen == [True]
