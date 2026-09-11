"""Real parent callers and SQLite receipts; no provider or external delivery."""
import asyncio
import queue
from unittest.mock import Mock

import pytest

from cli import HermesCLI
from hermes_state import SessionDB
from run_agent import AIAgent
from tools.process_registry import ProcessRegistry, ProcessSession


@pytest.fixture
def registry(monkeypatch, tmp_path):
    import tools.process_registry as module
    monkeypatch.setattr(module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    registry = ProcessRegistry()
    monkeypatch.setattr(module, "process_registry", registry)
    return registry


def observed(registry, owner="owner", sid="proc", consumed=True):
    event = dict(type="completion", session_id=sid, session_key=owner,
                 command="true", exit_code=0, output="done")
    registry._pending_terminal_entries[sid] = dict(session_id=sid, terminal_event=event)
    registry._finished[sid] = ProcessSession(id=sid, command="true", exited=True,
                                            exit_code=0, session_key=owner)
    if consumed:
        registry._completion_consumed.add(sid)
    registry.completion_queue.put(event)
    assert registry._write_checkpoint()
    return event


def real_agent(tmp_path, sid="owner"):
    db = SessionDB(tmp_path / "receipt.db")
    db.create_session(sid, source="cli")
    agent = AIAgent.__new__(AIAgent)
    agent.session_id = sid
    agent._session_db = db
    agent._session_db_created = True
    agent._last_flushed_db_idx = 0
    agent.max_iterations = 10
    return agent, db


def shell(agent):
    cli = HermesCLI.__new__(HermesCLI)
    cli.agent = agent
    cli.session_id = "owner"
    cli._session_db = agent._session_db
    cli.conversation_history = []
    cli._pending_input = queue.Queue()
    cli._ensure_runtime_credentials = lambda: True
    cli._active_agent_route_signature = "test"
    cli._resolve_turn_agent_config = lambda _: dict(signature="test", model="test", runtime={})
    cli._init_agent = lambda **kw: True
    for attr in ("_voice_mode", "_voice_continuous", "_voice_tts", "show_reasoning",
                 "_stream_started", "_stream_box_opened"):
        setattr(cli, attr, False)
    cli.final_response_markdown = "off"
    for attr in ("_reset_stream_state", "_flush_stream", "_flush_credit_notices",
                 "_ring_bell", "_emit_focus_recovery_line", "_transfer_session_yolo"):
        setattr(cli, attr, lambda *a, **kw: None)
    cli._scrollback_box_width = lambda *a: 80
    return cli


@pytest.mark.parametrize("outcome", ["success", "failed", "partial", "interrupted", "exception", "swallowed", "unpersisted", "early"])
def test_real_cli_chat_boundary(outcome, registry, tmp_path, monkeypatch):
    agent, db = real_agent(tmp_path)
    cli = shell(agent)
    observed(registry, consumed=False)
    observed(registry, "foreign", "foreign-proc")
    messages = [dict(role="user", content="check"), dict(role="assistant", content="done")]
    result = dict(completed=True, messages=messages, final_response="done", response_previewed=True)
    if outcome in {"failed", "partial", "interrupted"}:
        result[outcome] = True
    def run(**kw):
        assert registry.wait("proc", timeout=1)["status"] == "exited"
        if outcome == "exception":
            raise RuntimeError("provider failed")
        return result
    agent.run_conversation = Mock(side_effect=run)
    if outcome == "swallowed":
        cli._flush_stream = Mock(side_effect=RuntimeError("display failure"))
    if outcome == "early":
        cli._ensure_runtime_credentials = lambda: False
    if outcome == "unpersisted":
        # Exercise the REAL catching flush, with an actual SQLite write error.
        db._conn.execute("PRAGMA query_only=ON")
    cli._last_turn_durably_accepted = True  # stale receipt must never survive
    try:
        response = cli._chat_with_terminal_acceptance("check")
        assert cli._last_turn_durably_accepted is (outcome == "success")
        assert ("proc" not in registry._pending_terminal_entries) is (outcome == "success")
        assert "foreign-proc" in registry._pending_terminal_entries
        if outcome == "success":
            assert response == "done"
            assert [m["content"] for m in db.get_messages("owner")] == ["check", "done"]
            assert cli._record_chat_turn_acceptance(result)
            assert len(db.get_messages("owner")) == 2
        else:
            assert not registry.is_completion_consumed("proc")
            cli._last_turn_durably_accepted = True
            cli._finish_terminal_turn_acceptance(None, "owner")
            assert "proc" in registry._pending_terminal_entries
            cli._drain_process_notifications("cli-idle")
            assert cli._pending_input.get_nowait().event["session_id"] == "proc"
    finally:
        db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failed", "partial", "interrupted", "exception", "cancel", "unpersisted", "write-failure"])
async def test_real_gateway_parent_boundary(outcome, registry, tmp_path, monkeypatch):
    from tests.gateway.test_42039_duplicate_user_message import _bootstrap, _event, _source
    runner = _bootstrap(monkeypatch, tmp_path)
    runner._set_session_env = lambda *a, **kw: None
    runner._clear_session_env = lambda *a: None
    owner = "agent:main:telegram:group:-1001:12345"
    agent, db = real_agent(tmp_path, "sess-dedup")
    if outcome == "unpersisted":
        db._conn.execute("PRAGMA query_only=ON")
    observed(registry, owner, consumed=False)
    observed(registry, "foreign", "foreign-proc")
    async def run(*a, **kw):
        assert registry.read_log("proc")["status"] == "exited"
        if outcome == "exception":
            raise RuntimeError("provider failed")
        if outcome == "cancel":
            raise asyncio.CancelledError()
        result = dict(completed=True,
                      final_response="done", messages=[dict(role="assistant", content="done")])
        if outcome in {"failed", "partial", "interrupted"}:
            result[outcome] = True
        return gateway_result(agent, result)
    runner._run_agent = Mock(side_effect=run)
    if outcome == "write-failure":
        runner.session_store.append_to_transcript.side_effect = OSError("disk full")
    if outcome == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await runner._handle_message_with_agent(_event(), _source(), owner, 1)
    else:
        await runner._handle_message_with_agent(_event(), _source(), owner, 1)
    assert runner._run_agent.called
    assert ("proc" not in registry._pending_terminal_entries) is (outcome == "success")
    assert "foreign-proc" in registry._pending_terminal_entries
    if outcome != "success":
        assert not registry.is_completion_consumed("proc")
        runner._finish_terminal_observation_turn(owner, True)
        assert "proc" in registry._pending_terminal_entries
    else:
        assert [m["content"] for m in db.get_messages("sess-dedup")] == ["done"]
    db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("accepted", [False, True])
async def test_gateway_watcher_waits_for_parent_and_retries_only_ack(accepted, registry, tmp_path, monkeypatch):
    from tests.gateway.test_background_process_notifications import _build_runner, _watcher_dict
    from unittest.mock import AsyncMock
    owner = "owner"
    observed(registry, owner)
    registry._finished["proc"] = ProcessSession(id="proc", command="true", exited=True, exit_code=0, started_at=1)
    runner = _build_runner(monkeypatch, tmp_path, "all")
    runner._enqueue_process_completion_notification = AsyncMock(return_value=True)
    original_write = registry._write_checkpoint
    writes = []
    def write():
        writes.append(True)
        return False if len(writes) == 1 else original_write()
    monkeypatch.setattr(registry, "_write_checkpoint", write)
    watcher = _watcher_dict("proc")
    watcher.update(notify_on_complete=True, session_key=owner)
    task = asyncio.create_task(runner._run_process_watcher(watcher))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert not task.done()
    with registry._lock:
        registry._prune_if_needed()
    assert "proc" in registry._finished
    runner._finish_terminal_observation_turn(owner, accepted)
    await asyncio.wait_for(task, 5)
    assert "proc" not in registry._pending_terminal_entries
    assert len(writes) == 2
    assert runner._enqueue_process_completion_notification.await_count == (0 if accepted else 1)


def gateway_result(agent, result):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from gateway.run import TurnRunner
    from gateway.turn_context import TurnContext
    from gateway.session import SessionSource
    from gateway.config import Platform
    runner = MagicMock()
    runner.config = SimpleNamespace(streaming=None)
    runner._provider_routing = {}
    runner._agent_cache_lock = None
    runner._agent_cache = {}
    runner._session_db = None
    runner._prefill_messages = None
    runner._pending_model_notes = {}
    runner._pending_skills_reload_notes = {}
    runner.session_store._entries = {}
    runner._get_system_prompt_for_channel.return_value = None
    runner._resolve_session_agent_runtime.return_value = ("test-model", {})
    runner._resolve_session_reasoning_config.return_value = None
    runner._resolve_session_service_tier.return_value = None
    runner._resolve_turn_agent_config.return_value = dict(model="test-model", runtime={})
    runner._agent_config_signature.return_value = ("test",)
    runner._extract_cache_busting_config.return_value = {}
    runner._refresh_fallback_model.return_value = None
    runner._consume_pending_native_image_paths.return_value = []
    runner._consume_pending_turn_sidecar_notes.return_value = []
    runner._is_telegram_topic_lane.return_value = False
    runner._is_discord_auto_thread_lane.return_value = False
    runner._is_relay_discord_channel_lane.return_value = False
    agent.model = "test-model"
    agent.tools = []
    agent.context_compressor = SimpleNamespace(last_prompt_tokens=0, context_length=100000)
    agent.session_prompt_tokens = agent.session_completion_tokens = 0
    agent.run_conversation = Mock(return_value=result)
    ctx = TurnContext(source=SessionSource(platform=Platform.LOCAL, chat_id="test", user_id="test"),
                      message="check", history=[], session_id=agent.session_id, session_key="owner",
                      user_config={}, AIAgent=lambda **kw: agent,
                      resolve_display_setting=lambda *a: False,
                      _run_still_current=lambda: True, _hooks_ref=SimpleNamespace(loaded_hooks=False))
    return TurnRunner(runner, ctx).run_sync()


@pytest.mark.parametrize("writable", [False, True])
def test_real_gateway_run_sync_receipt(registry, tmp_path, writable):
    agent, db = real_agent(tmp_path)
    if not writable:
        db._conn.execute("PRAGMA query_only=ON")
    messages = [dict(role="user", content="check"), dict(role="assistant", content="done")]
    try:
        result = gateway_result(agent, dict(completed=True, messages=messages, final_response="done"))
        assert result["turn_persisted"] is writable
        assert len(db.get_messages("owner")) == (2 if writable else 0)
    finally:
        db.close()


def test_cli_accepted_observation_ack_failure_uses_idle_drain(registry, tmp_path, monkeypatch):
    agent, db = real_agent(tmp_path)
    cli = shell(agent)
    observed(registry)
    original = registry._write_checkpoint
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: False)
    cli._last_turn_durably_accepted = True
    assert not cli._finish_terminal_turn_acceptance(None, "owner")
    assert registry._accepted_terminal_ack_retries == {"proc"}
    # A later rejected turn cannot re-arm the already accepted observation.
    cli._last_turn_durably_accepted = False
    cli._finish_terminal_turn_acceptance(None, "owner")
    monkeypatch.setattr(registry, "_write_checkpoint", original)
    cli._drain_process_notifications("cli-idle")
    assert cli._pending_input.empty()
    assert not registry._pending_terminal_entries
    db.close()


def test_cli_real_startup_recovery_routes_only_resumed_owner(registry, tmp_path, monkeypatch):
    observed(registry, consumed=False)
    observed(registry, "foreign", "foreign-proc", consumed=False)
    fresh = ProcessRegistry()
    monkeypatch.setattr("tools.process_registry.process_registry", fresh)
    agent, db = real_agent(tmp_path)
    cli = shell(agent)
    cli._claim_active_session = lambda _: True
    cli.show_banner = Mock(side_effect=RuntimeError("stop before UI"))
    with pytest.raises(RuntimeError, match="stop before UI"):
        cli.run()
    cli._drain_process_notifications("cli-idle")
    assert cli._pending_input.get_nowait().event["session_id"] == "proc"
    assert cli._pending_input.empty()
    assert fresh.completion_queue.get_nowait()["session_id"] == "foreign-proc"
    db.close()


def test_failed_synthetic_cli_turn_replays_but_not_into_new_session(registry, tmp_path):
    observed(registry, consumed=False)
    agent, db = real_agent(tmp_path)
    cli = shell(agent)
    cli._ensure_runtime_credentials = lambda: False
    cli._drain_process_notifications("cli-idle")
    pending = cli._pending_input.get_nowait()
    assert cli._chat_with_terminal_acceptance(pending.text, pending) is None
    cli._drain_process_notifications("cli-idle")
    pending = cli._pending_input.get_nowait()
    assert pending.event["session_id"] == "proc"
    cli.session_id = "new-session"
    cli.chat = Mock(side_effect=AssertionError("must not replay into /new"))
    assert cli._chat_with_terminal_acceptance(pending.text, pending) is None
    cli.chat.assert_not_called()
    assert "proc" in registry._pending_terminal_entries
    db.close()


def test_observation_token_rejects_late_worker_and_unrelated_acceptance(registry):
    from contextvars import copy_context
    from tools.process_registry import begin_terminal_observation_turn, end_terminal_observation_turn
    observed(registry, consumed=False)
    observed(registry, sid="old", consumed=True)
    observed(registry, "foreign", "foreign-proc", consumed=False)
    turn, token = begin_terminal_observation_turn("owner")
    worker_context = copy_context()
    registry.read_log("proc")
    registry.read_log("foreign-proc")
    end_terminal_observation_turn(turn, token)
    registry.release_consumed_terminal_notifications("owner", turn["observed"])
    assert turn["observed"] == {"proc"}
    newer, newer_token = begin_terminal_observation_turn("owner")
    worker_context.run(registry.read_log, "proc")
    end_terminal_observation_turn(newer, newer_token)
    registry.acknowledge_consumed_terminal_notifications("owner", newer["observed"])
    assert not registry.is_completion_consumed("proc")
    assert not registry.is_completion_consumed("foreign-proc")
    assert set(registry._pending_terminal_entries) == {"proc", "old", "foreign-proc"}


@pytest.mark.asyncio
async def test_gateway_idle_watcher_retries_accepted_io_without_delivery(registry, monkeypatch):
    from gateway.run import GatewayRunner
    observed(registry)
    writer = registry._write_checkpoint
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: False)
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._finish_terminal_observation_turn("owner", True, {"proc"})
    monkeypatch.setattr(registry, "_write_checkpoint", writer)
    runner._running = True
    async def tick(_):
        if not registry._pending_terminal_entries:
            runner._running = False
    monkeypatch.setattr(asyncio, "sleep", tick)
    await runner._async_delegation_watcher()
    assert not registry._pending_terminal_entries
    assert not registry._accepted_terminal_ack_retries


@pytest.mark.asyncio
@pytest.mark.parametrize("finishes", [False, True])
async def test_real_graceful_drain_includes_bounded_notify_not_servers(registry, finishes):
    from gateway.run import GatewayRunner
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running_agents = {}
    runner._snapshot_running_agents = lambda: {}
    for name in ("_running_agent_count", "_active_cron_job_count", "_active_api_run_count", "_active_deferred_agent_worker_count"):
        setattr(runner, name, lambda: 0)
    runner._update_runtime_status = lambda _: None
    registry._running["server"] = ProcessSession(id="server", command="server", notify_on_complete=False)
    assert await runner._drain_active_agents(.05) == ({}, False)
    work = ProcessSession(id="bounded", command="test", notify_on_complete=True)
    registry._running[work.id] = work
    if finishes:
        asyncio.get_running_loop().call_soon(setattr, work, "exited", True)
    _, timed_out = await asyncio.wait_for(runner._drain_active_agents(.05), 5)
    assert timed_out is (not finishes)


@pytest.mark.parametrize("batch", [False, True])
def test_executor_construction_failure_removes_phantom_dispatch(monkeypatch, tmp_path, batch):
    import tools.async_delegation as ad
    monkeypatch.setattr(ad, "_db_path", lambda: tmp_path / "dispatch.db")
    monkeypatch.setattr(ad, "_records", {})
    monkeypatch.setattr(ad, "_get_executor", Mock(side_effect=RuntimeError("pool unavailable")))
    worker = Mock(side_effect=AssertionError("must not start"))
    dispatch = ad.dispatch_async_delegation_batch if batch else ad.dispatch_async_delegation
    result = dispatch(**({"goals": ["test"]} if batch else {"goal": "test"}),
                      context=None, toolsets=None, role="subagent", model=None,
                      session_key="owner", runner=worker)
    assert result["status"] == "rejected"
    assert not ad._records
    worker.assert_not_called()
    with ad._transaction() as conn:
        assert conn.execute("SELECT COUNT(*) FROM async_delegations").fetchone()[0] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "partial", "interrupted", "unpersisted", "exception", "busy", "no-handler"])
async def test_real_adapter_enqueue_waits_for_durable_parent(outcome, registry, tmp_path, monkeypatch):
    from gateway.platforms.base import BasePlatformAdapter, SendResult
    from gateway.config import PlatformConfig, Platform
    from tests.gateway.test_42039_duplicate_user_message import _bootstrap, _source
    class LocalAdapter(BasePlatformAdapter):
        async def connect(self, **kw): return True
        async def disconnect(self): pass
        async def send(self, *a, **kw): return SendResult(success=True)
        async def get_chat_info(self, chat_id): return {}
    adapter = LocalAdapter(PlatformConfig(enabled=True), Platform.TELEGRAM)
    adapter.config.typing_indicator = False
    runner = _bootstrap(monkeypatch, tmp_path)
    runner.adapters = {Platform.TELEGRAM: adapter}
    runner._set_session_env = lambda *a, **kw: None
    runner._clear_session_env = lambda *a: None
    runner._build_process_event_source = lambda _: _source()
    runner._completion_notification_batch_window = 0
    owner = "agent:main:telegram:group:-1001:12345"
    agent, db = real_agent(tmp_path, "sess-dedup")
    if outcome == "unpersisted":
        db._conn.execute("PRAGMA query_only=ON")
    entered, finish = asyncio.Event(), asyncio.Event()
    async def run(*a, **kw):
        entered.set()
        await finish.wait()
        if outcome == "exception":
            raise RuntimeError("provider failed")
        result = dict(completed=True, final_response="done", messages=[dict(role="assistant", content="done")])
        if outcome in {"partial", "interrupted"}:
            result[outcome] = True
        return gateway_result(agent, result)
    runner._run_agent = Mock(side_effect=run)
    async def handler(event):
        return await runner._handle_message_with_agent(event, event.source, owner, 1)
    if outcome != "no-handler":
        adapter.set_message_handler(handler)
    if outcome == "busy":
        from gateway.session import build_session_key
        key = build_session_key(_source())
        adapter._active_sessions[key] = asyncio.Event()
        adapter._session_tasks[key] = asyncio.current_task()
    event = observed(registry, owner, consumed=False)
    event.update(platform="telegram", chat_id="-1001", user_id="12345", chat_type="group", started_at=1)
    task = asyncio.create_task(runner._enqueue_process_completion_notification("done", event))
    try:
        if outcome not in {"busy", "no-handler"}:
            await asyncio.wait_for(entered.wait(), 10)
            assert not task.done()  # enqueue cannot acknowledge mere scheduling
            assert "proc" in registry._pending_terminal_entries
            finish.set()
        delivered = await asyncio.wait_for(task, 10)
        assert delivered is (outcome == "success")
        if delivered:
            assert registry.acknowledge_terminal_notification("proc")
        assert ("proc" not in registry._pending_terminal_entries) is (outcome == "success")
        if outcome == "busy":
            assert not adapter._pending_messages
    finally:
        finish.set()
        if not task.done():
            task.cancel()
        db.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["parent_session_id", "origin_profile", "origin_hermes_home"])
async def test_real_batch_enqueue_separates_parent_identity(field):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from tests.gateway.test_completion_delivery import _runner, _completion_event
    adapter = SimpleNamespace(handle_message=AsyncMock())
    runner = _runner(adapter)
    runner._classify_completion_target = AsyncMock(return_value="deliver")
    runner._completion_notification_batch_window = .01
    first = _completion_event(started_at=1, session_id="proc-one")
    second = _completion_event(started_at=2, session_id="proc-two")
    first[field], second[field] = "one", "two"
    results = await asyncio.gather(
        runner._enqueue_process_completion_notification("one", first),
        runner._enqueue_process_completion_notification("two", second),
    )
    assert results == [True, True]
    assert adapter.handle_message.await_count == 2


@pytest.mark.asyncio
async def test_closed_parent_disposition_write_failure_retries(registry, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    from tests.gateway.test_completion_delivery import _runner
    runner = _runner(SimpleNamespace(handle_message=AsyncMock()))
    runner._classify_completion_target = AsyncMock(return_value="terminal")
    event = observed(registry, consumed=False)
    event["parent_session_id"] = "closed"
    writer = registry._write_checkpoint
    monkeypatch.setattr(registry, "_write_checkpoint", lambda: False)
    assert await runner._deliver_completion_notification("done", event) is False
    monkeypatch.setattr(registry, "_write_checkpoint", writer)
    assert await runner._deliver_completion_notification("done", event) is None
    assert registry._pending_terminal_entries["proc"]["terminal_event"]["delivery_terminal"]
