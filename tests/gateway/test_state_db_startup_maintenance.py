"""Gateway readiness must precede every heavyweight state.db search pass."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path

import pytest

from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter
from gateway.run import GatewayRunner
from hermes_state import (
    SessionDB,
    _default_db_path,
    close_shared_session_dbs,
    get_shared_session_db,
    release_shared_session_db,
)
from hermes_state_common import _FTS_TRIGGERS, SCHEMA_VERSION


class _ConnectedAdapter(BasePlatformAdapter):
    def __init__(self, connected: threading.Event):
        super().__init__(PlatformConfig(enabled=True, token="***"), Platform.DISCORD)
        self._connected_event = connected

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        self._mark_connected()
        self._connected_event.set()
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        return None

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


def _make_current_complete_db(db_path: Path) -> None:
    """Match the incident shape: v30, six triggers, marker, no stale flag."""
    db = SessionDB(db_path)
    if not db._fts_enabled or not db._trigram_available:
        db.close()
        pytest.skip("FTS5 with trigram support is unavailable in this SQLite build")
    db.create_session("existing", "discord")
    db.append_message("existing", "user", "already indexed")
    rows = db._conn.execute(
        "SELECT name FROM sqlite_master WHERE type='trigger' AND name IN (%s)"
        % ",".join("?" for _ in _FTS_TRIGGERS),
        _FTS_TRIGGERS,
    ).fetchall()
    assert {row[0] for row in rows} == set(_FTS_TRIGGERS)
    assert (
        db._conn.execute("SELECT version FROM schema_version").fetchone()[0]
        == SCHEMA_VERSION
    )
    assert db._conn.execute(
        "SELECT 1 FROM state_meta WHERE key='fts_tool_full_content_high_water'"
    ).fetchone()
    assert not db._conn.execute(
        "SELECT 1 FROM state_meta WHERE key='fts_stale'"
    ).fetchone()
    db.close()


def test_deferred_current_schema_open_has_durable_writes_and_like_search(tmp_path):
    db_path = tmp_path / "state.db"
    _make_current_complete_db(db_path)

    db = SessionDB(db_path, defer_fts_initialization=True)
    try:
        assert db.state_db_maintenance_pending is True
        assert db._fts_enabled is False
        db.append_message("existing", "user", "written before maintenance")
        rows = db.search_messages("written before maintenance")
        assert any("written before maintenance" in row["snippet"] for row in rows)

        assert db.run_deferred_startup_maintenance(threading.Event()) is True
        assert db.state_db_maintenance_pending is False
        assert db.run_deferred_startup_maintenance(threading.Event()) is True
        assert db._fts_enabled is True
        indexed = db._conn.execute(
            "SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'written'"
        ).fetchall()
        assert indexed
    finally:
        db.close()


@pytest.mark.asyncio
async def test_adapter_connect_precedes_blocked_current_schema_fts_probe_and_write_survives(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    db_path = Path(_default_db_path())
    _make_current_complete_db(db_path)

    connected = threading.Event()
    probe_started = threading.Event()
    release_probe = threading.Event()
    original_probe = SessionDB._fts_table_probe

    def blocked_ordinary_probe(self, cursor, table_name):
        assert connected.is_set(), "ordinary FTS probing ran before adapter connect"
        probe_started.set()
        assert release_probe.wait(30)
        return original_probe(self, cursor, table_name)

    # This probe runs on every healthy current-schema open. It is not a
    # missing-trigger or stale-index rebuild seam.
    monkeypatch.setattr(SessionDB, "_fts_table_probe", blocked_ordinary_probe)
    config = GatewayConfig(
        platforms={Platform.DISCORD: PlatformConfig(enabled=True, token="***")},
        sessions_dir=tmp_path / "sessions",
    )
    runner = GatewayRunner(config)
    assert not probe_started.is_set(), "GatewayRunner construction ran heavy FTS work"
    adapter = _ConnectedAdapter(connected)
    monkeypatch.setattr(runner, "_create_adapter", lambda *_args: adapter)

    async def no_secondary_profiles():
        return 0

    monkeypatch.setattr(
        runner, "_start_secondary_profile_adapters", no_secondary_profiles
    )
    db = runner.session_store._db
    db.create_session("during", "discord")
    db.append_message("during", "user", "accepted before adapter startup")

    try:
        assert await runner.start() is True
        assert connected.is_set()
        assert await asyncio.to_thread(probe_started.wait, 2)

        pending_write = asyncio.create_task(
            asyncio.to_thread(db.append_message, "during", "user", "not lost")
        )
        await asyncio.sleep(0.05)
        assert not pending_write.done(), "write raced FTS reconciliation lock"

        release_probe.set()
        await asyncio.wait_for(pending_write, 5)
        contents = [
            row[0]
            for row in db._conn.execute(
                "SELECT content FROM messages WHERE session_id='during' ORDER BY id"
            )
        ]
        assert contents == ["accepted before adapter startup", "not lost"]
    finally:
        release_probe.set()
        await runner.stop()


@pytest.mark.asyncio
async def test_background_fts_failure_degrades_search_not_platforms(
    monkeypatch,
    tmp_path,
    caplog,
):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner._background_tasks = set()
    runner._state_db_maintenance_cancel = threading.Event()
    status_updates = []
    monkeypatch.setattr(
        "gateway.status.write_runtime_status",
        lambda **kwargs: status_updates.append(kwargs),
    )
    db = SessionDB(tmp_path / "state.db", defer_fts_initialization=True)
    db.create_session("survives", "discord")
    db.append_message("survives", "user", "canonical row survives")

    def fail_after_enabling_search():
        db._fts_enabled = True
        raise RuntimeError("reconciliation failed")

    monkeypatch.setattr(db, "_init_schema", fail_after_enabling_search)
    try:
        with caplog.at_level("ERROR"):
            assert await runner._run_state_db_maintenance(db) is False

        assert runner._running is True
        assert db.state_db_maintenance_pending is True
        assert db._fts_enabled is False
        assert db.search_messages("canonical row survives")
        assert "reconciliation failed" in caplog.text
        assert status_updates[-1]["session_store"] == {
            "search_status": "failed",
            "maintenance_error": "reconciliation failed",
        }
    finally:
        db.close()


def test_search_status_update_preserves_unavailable_core_store(monkeypatch, tmp_path):
    from gateway import status

    status_path = tmp_path / "runtime.json"
    monkeypatch.setattr(status, "_get_runtime_status_path", lambda: status_path)
    status.write_runtime_status(session_store={"status": "unavailable"})
    status.write_runtime_status(
        session_store={"search_status": "failed", "maintenance_error": "probe failed"}
    )

    payload = json.loads(status_path.read_text())
    assert payload["session_store"] == {
        "status": "unavailable",
        "search_status": "failed",
        "maintenance_error": "probe failed",
    }


def test_existing_shared_generation_does_not_duplicate_or_force_maintenance(
    monkeypatch,
    tmp_path,
):
    path = tmp_path / "state.db"
    normal_path = tmp_path / "normal-first.db"
    close_shared_session_dbs()
    normal_first = get_shared_session_db(normal_path)
    first = get_shared_session_db(path, defer_fts_initialization=True)
    try:
        assert first.state_db_maintenance_pending
        second = get_shared_session_db(path)
        try:
            assert second is first
            assert second.state_db_maintenance_pending
        finally:
            release_shared_session_db(second)

        first.run_deferred_startup_maintenance(threading.Event())
        monkeypatch.setattr(
            SessionDB,
            "_fts_table_probe",
            lambda *_args: pytest.fail(
                "gateway acquire repeated completed maintenance"
            ),
        )
        gateway_acquire = get_shared_session_db(path, defer_fts_initialization=True)
        try:
            assert gateway_acquire is first
            assert not gateway_acquire.state_db_maintenance_pending
        finally:
            release_shared_session_db(gateway_acquire)

        gateway_after_normal = get_shared_session_db(
            normal_path, defer_fts_initialization=True
        )
        try:
            assert gateway_after_normal is normal_first
            assert not gateway_after_normal.state_db_maintenance_pending
        finally:
            release_shared_session_db(gateway_after_normal)
    finally:
        release_shared_session_db(first)
        release_shared_session_db(normal_first)
        close_shared_session_dbs()


@pytest.mark.asyncio
async def test_multiplex_watcher_maintains_profile_handle_opened_late(tmp_path):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner._state_db_maintenance_cancel = threading.Event()
    runner.config = GatewayConfig(multiplex_profiles=True)

    class PendingDB:
        def __init__(self, name):
            self.db_path = tmp_path / name / "state.db"
            self.state_db_maintenance_pending = True

    default_db = PendingDB("default")
    late_profile_db = PendingDB("profile")
    handles = [default_db]
    maintained = []

    runner._state_db_maintenance_config = lambda: {}
    runner._state_db_handles_snapshot = lambda: list(handles)

    async def run_one(db, _config):
        maintained.append(db)
        db.state_db_maintenance_pending = False
        if db is default_db:
            handles.append(late_profile_db)
        else:
            runner._state_db_maintenance_cancel.set()
        return True

    runner._run_state_db_maintenance = run_one
    runner._wait_for_state_db_maintenance_cancel = lambda _timeout: asyncio.sleep(0)

    await runner._state_db_maintenance_watcher()

    assert maintained == [default_db, late_profile_db]


@pytest.mark.asyncio
async def test_gateway_shutdown_awaits_maintenance_before_db_close(
    monkeypatch, tmp_path
):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    stopped = threading.Event()
    started = threading.Event()
    closed = threading.Event()

    class BlockingDB:
        db_path = tmp_path / "blocked-state.db"
        state_db_maintenance_pending = True

        def run_deferred_startup_maintenance(self, cancel_event):
            started.set()
            assert cancel_event.wait(2)
            stopped.set()
            return False

        def close(self):
            assert stopped.is_set(), (
                "SessionDB closed before maintenance thread stopped"
            )
            assert runner._state_db_maintenance_task.done()
            closed.set()

    runner = GatewayRunner(
        GatewayConfig(platforms={}, sessions_dir=tmp_path / "sessions")
    )
    fake_db = BlockingDB()
    with runner.session_store._db_handles_lock:
        runner.session_store._db_handles.clear()
        runner.session_store._db_handles[fake_db.db_path] = fake_db
    runner.session_store._db = fake_db

    await runner.start()
    assert await asyncio.to_thread(started.wait, 2)
    await runner.stop()

    assert stopped.is_set()
    assert closed.is_set()
