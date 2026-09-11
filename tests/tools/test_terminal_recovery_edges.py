"""Retained outcomes, failed PTY admission, and sandbox restart recovery."""
import json
import sys
from types import SimpleNamespace
import pytest


def test_closed_outcome_evidence_still_counts_toward_capacity(monkeypatch, tmp_path):
    import tools.process_registry as module
    monkeypatch.setattr(module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(module, "MAX_PENDING_TERMINAL_NOTIFICATIONS", 1)
    registry = module.ProcessRegistry()
    registry._pending_terminal_entries["closed"] = {"session_id": "closed", "terminal_event": {"delivery_terminal": True}}
    assert registry.reserve_notify_spawn() is False
    assert "closed" in registry._pending_terminal_entries


def test_pty_checkpoint_failure_kills_spawned_child(monkeypatch, tmp_path):
    import tools.process_registry as module
    registry = module.ProcessRegistry()
    monkeypatch.setattr(module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(module, "_find_shell", lambda: "/bin/sh")
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: False)
    killed = []
    # ptyprocess.PtyProcess.kill requires an explicit signal argument.
    child = SimpleNamespace(pid=12345, kill=lambda sig: killed.append(sig))
    monkeypatch.setitem(sys.modules, "ptyprocess", SimpleNamespace(PtyProcess=SimpleNamespace(spawn=lambda *a, **k: child)))
    with pytest.raises(module.CheckpointPersistenceError):
        registry.spawn_local("sleep 60", str(tmp_path), use_pty=True, notify_on_complete=True)
    assert killed
    assert not registry._running


def test_sandbox_restart_retains_unknown_outcome_without_host_probe(monkeypatch, tmp_path):
    import tools.process_registry as module
    checkpoint = tmp_path / "processes.json"
    monkeypatch.setattr(module, "CHECKPOINT_PATH", checkpoint)
    checkpoint.write_text(json.dumps([{"session_id": "sandbox", "pid": 12345, "pid_scope": "sandbox", "notify_on_complete": True, "session_key": "owner", "watcher_thread_id": "thread"}]))
    registry = module.ProcessRegistry()
    def forbidden(*args):
        raise AssertionError("sandbox PID must never be probed on host")
    monkeypatch.setattr(registry, "_host_pid_is_ours", forbidden)
    monkeypatch.setattr(registry, "_is_host_pid_alive", forbidden)
    registry.recover_from_checkpoint()
    event = registry.completion_queue.get_nowait()
    assert event["completion_reason"] == "interrupted_unknown"
    assert registry._finished["sandbox"].pid_scope == "sandbox"
    assert json.loads(checkpoint.read_text())[0]["terminal_event"]["session_id"] == "sandbox"
