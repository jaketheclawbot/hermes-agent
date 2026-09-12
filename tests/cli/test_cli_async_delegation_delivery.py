"""Regression coverage for CLI async-delegation completion ownership."""

import queue

import pytest

from cli import HermesCLI, _DurableCompletionMessage


def test_cli_completion_drain_uses_visible_session_identity(monkeypatch):
    """A CLI window must not claim another window's restored completion."""
    cli = HermesCLI.__new__(HermesCLI)
    cli.session_id = "visible-session"
    cli._pending_input = queue.Queue()

    event = {
        "type": "async_delegation",
        "delegation_id": "deleg_visible",
        "session_key": "visible-session",
    }
    calls = []

    class FakeRegistry:
        def drain_notifications(self, *, session_key="", owns_event=None):
            calls.append((session_key, owns_event(event)))
            return [(event, "completion payload")]

    claimed = []
    completed = []

    monkeypatch.setattr(
        "tools.process_registry.process_registry",
        FakeRegistry(),
    )
    monkeypatch.setattr(
        "tools.async_delegation.claim_event_delivery",
        lambda evt, consumer: claimed.append((evt, consumer)) or "claim-token",
    )
    monkeypatch.setattr(
        "tools.async_delegation.complete_event_delivery",
        lambda evt, token: completed.append((evt, token)),
    )

    cli._drain_process_notifications("cli-idle")

    assert calls == [("visible-session", True)]
    pending = cli._pending_input.get_nowait()
    assert pending.text == "completion payload"
    assert claimed == [(event, "cli-idle")]
    assert completed == []

    cli._acknowledge_durable_completion(pending)
    assert completed == [(event, "claim-token")]


def test_cli_completion_ownership_rejects_foreign_session():
    cli = HermesCLI.__new__(HermesCLI)
    cli.session_id = "visible-session"
    cli._session_db = None

    assert not cli._owns_process_notification(
        {"type": "async_delegation", "session_key": "foreign-session"}
    )


def test_cli_completion_ownership_accepts_compression_lineage():
    cli = HermesCLI.__new__(HermesCLI)
    cli.session_id = "visible-session"

    class FakeSessionDB:
        def resolve_resume_session_id(self, session_id):
            assert session_id == "pre-compression-session"
            return "visible-session"

    cli._session_db = FakeSessionDB()

    assert cli._owns_process_notification(
        {
            "type": "async_delegation",
            "session_key": "pre-compression-session",
        }
    )


@pytest.mark.parametrize("result", [
    {"completed": False},
    {"completed": True, "failed": True},
    {"completed": True, "partial": True},
    {"completed": True, "interrupted": True},
])
def test_cli_turn_receipt_rejects_failed_partial_and_interrupted(result):
    cli = HermesCLI.__new__(HermesCLI)
    cli.conversation_history = [{"role": "assistant", "content": "x"}]
    cli.agent = type("Agent", (), {
        "_flush_messages_to_session_db": lambda *args: True,
    })()

    assert cli._record_chat_turn_acceptance(result) is False
    assert cli._last_turn_durably_accepted is False


def test_cli_turn_receipt_requires_successful_persistence():
    cli = HermesCLI.__new__(HermesCLI)
    cli.conversation_history = [{"role": "assistant", "content": "x"}]
    result = {"completed": True, "failed": False, "partial": False,
              "interrupted": False}
    cli.agent = type("Agent", (), {
        "_flush_messages_to_session_db": lambda *args: False,
    })()
    assert cli._record_chat_turn_acceptance(result) is False

    cli.agent = type("Agent", (), {
        "_flush_messages_to_session_db": lambda *args: True,
    })()
    assert cli._record_chat_turn_acceptance(result) is True


def test_failed_synthetic_turn_releases_claim_and_rearms_owner(monkeypatch):
    cli = HermesCLI.__new__(HermesCLI)
    cli._last_turn_durably_accepted = False
    event = {"type": "async_delegation", "session_key": "owner"}
    message = _DurableCompletionMessage("payload", event, "claim")
    released = []
    rearmed = []
    monkeypatch.setattr(
        "tools.async_delegation.release_event_delivery",
        lambda evt, claim: released.append((evt, claim)),
    )
    monkeypatch.setattr(
        "tools.process_registry.process_registry.release_consumed_terminal_notifications",
        lambda owner: rearmed.append(owner),
    )

    assert cli._finish_terminal_turn_acceptance(message, "owner") is False
    assert released == [(event, "claim")]
    assert rearmed == ["owner"]


def test_successful_turn_acks_original_owner_after_session_rotation(monkeypatch):
    cli = HermesCLI.__new__(HermesCLI)
    cli._last_turn_durably_accepted = True
    cli.session_id = "compressed-child"
    acked = []
    monkeypatch.setattr(
        "tools.process_registry.process_registry.acknowledge_consumed_terminal_notifications",
        lambda owner: acked.append(owner) or True,
    )

    assert cli._finish_terminal_turn_acceptance(None, "original-owner") is True
    assert acked == ["original-owner"]


def test_cli_run_wires_checkpoint_recovery_before_ui(monkeypatch):
    cli = HermesCLI.__new__(HermesCLI)
    cli._claim_active_session = lambda _owner: True
    cli.show_banner = lambda: (_ for _ in ()).throw(RuntimeError("stop after recovery"))
    recovered = []
    monkeypatch.setattr(
        "tools.process_registry.process_registry.recover_from_checkpoint",
        lambda: recovered.append(True) or 1,
    )

    with pytest.raises(RuntimeError, match="stop after recovery"):
        cli.run()
    assert recovered == [True]


def test_failed_observation_turn_reenters_real_idle_drain(monkeypatch):
    from tools.process_registry import ProcessRegistry

    registry = ProcessRegistry()
    event = {
        "type": "completion", "session_id": "proc-observed",
        "session_key": "owner", "command": "true", "exit_code": 0,
        "output": "done",
    }
    registry._pending_terminal_entries["proc-observed"] = {
        "session_id": "proc-observed", "terminal_event": event,
    }
    registry._completion_consumed.add("proc-observed")
    registry.completion_queue.put(event)
    monkeypatch.setattr("tools.process_registry.process_registry", registry)
    monkeypatch.setattr(
        "tools.async_delegation.claim_event_delivery",
        lambda _event, _consumer: "claim",
    )

    cli = HermesCLI.__new__(HermesCLI)
    cli.session_id = "owner"
    cli._session_db = None
    cli._pending_input = queue.Queue()
    cli._last_turn_durably_accepted = False

    # Tool observation suppresses the queued completion until its parent turn
    # is accepted. A failed parent turn re-arms that same durable event.
    cli._drain_process_notifications("cli-idle")
    assert cli._pending_input.empty()
    assert cli._finish_terminal_turn_acceptance(None, "owner") is False
    cli._drain_process_notifications("cli-idle")
    pending = cli._pending_input.get_nowait()
    assert pending.event["session_id"] == "proc-observed"

    # A later successful turn for the owner accepts exactly that observation.
    cli._last_turn_durably_accepted = True
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: True)
    assert cli._finish_terminal_turn_acceptance(pending, "owner") is True
    assert "proc-observed" not in registry._pending_terminal_entries


def test_cli_terminal_ack_write_retries_without_duplicate_turn(monkeypatch):
    cli = HermesCLI.__new__(HermesCLI)
    cli._last_turn_durably_accepted = True
    event = {"type": "completion", "session_id": "proc-ack",
             "session_key": "owner"}
    message = _DurableCompletionMessage("payload", event, "claim")
    completed = []
    ack_results = iter([False, True])
    acked = []
    monkeypatch.setattr(
        "tools.async_delegation.complete_event_delivery",
        lambda evt, claim: completed.append((evt, claim)),
    )
    monkeypatch.setattr(
        "tools.process_registry.process_registry.acknowledge_terminal_notification",
        lambda sid: acked.append(sid) or next(ack_results),
    )
    monkeypatch.setattr(
        "tools.process_registry.process_registry.acknowledge_consumed_terminal_notifications",
        lambda _owner: True,
    )

    assert cli._finish_terminal_turn_acceptance(message, "owner") is True
    assert cli._pending_terminal_ack_retries == {"proc-ack"}
    cli._retry_terminal_acknowledgements()
    assert cli._pending_terminal_ack_retries == set()
    assert completed == [(event, "claim")]
    assert acked == ["proc-ack", "proc-ack"]
