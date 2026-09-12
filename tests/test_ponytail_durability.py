import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tools import process_registry as pr
from tools import async_delegation as ad


class PonytailDurabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.checkpoint = Path(self.tmp.name) / "processes.json"
        self.path_patch = patch.object(pr, "CHECKPOINT_PATH", self.checkpoint)
        self.path_patch.start()

    def tearDown(self):
        self.path_patch.stop()
        self.tmp.cleanup()

    def test_dead_checkpoint_replays_until_acknowledged(self):
        registry = pr.ProcessRegistry()
        child = subprocess.Popen(["/bin/sleep", "0.05"])
        started = registry._safe_host_start_time(child.pid)
        child.wait()
        self.checkpoint.write_text(json.dumps([{
            "session_id": "proc_dead", "command": "/bin/sleep 0.05",
            "pid": child.pid, "pid_scope": "host", "host_start_time": started,
            "task_id": "task", "notify_on_complete": True,
            "session_key": "discord:chat", "watcher_platform": "discord",
            "watcher_chat_id": "chat", "watcher_interval": .05,
        }]))
        self.assertEqual(registry.recover_from_checkpoint(), 0)
        event = registry.completion_queue.get_nowait()
        self.assertEqual(event["completion_reason"], "interrupted_unknown")
        self.assertEqual(len(registry.pending_watchers), 1)
        self.assertTrue(json.loads(self.checkpoint.read_text())[0]["terminal_event"])

        fresh = pr.ProcessRegistry()
        fresh.recover_from_checkpoint()
        replay = fresh.completion_queue.get_nowait()
        self.assertTrue(replay["restored"])
        fresh.acknowledge_terminal_notification("proc_dead")
        self.assertEqual(json.loads(self.checkpoint.read_text()), [])

    def test_live_terminal_outcome_is_persisted_before_queue_and_ack(self):
        registry = pr.ProcessRegistry()
        session = pr.ProcessSession(
            id="proc_live", command="true", task_id="task",
            session_key="discord:chat", notify_on_complete=True,
        )
        registry._running[session.id] = session
        session.exited = True
        session.exit_code = 0
        registry._move_to_finished(session)
        saved = json.loads(self.checkpoint.read_text())
        self.assertEqual(saved[0]["terminal_event"]["session_id"], "proc_live")
        self.assertEqual(registry.active_notify_process_count(), 0)
        registry.acknowledge_terminal_notification("proc_live")
        self.assertEqual(json.loads(self.checkpoint.read_text()), [])

    def test_real_running_checkpoint_forwards_redactor_keywords(self):
        registry = pr.ProcessRegistry()
        child = subprocess.Popen(["/bin/sleep", "0.1"])
        try:
            registry._running["proc_running"] = pr.ProcessSession(
                id="proc_running", command="echo secret", pid=child.pid,
                host_start_time=registry._safe_host_start_time(child.pid),
            )
            self.assertTrue(registry._write_checkpoint())
            saved = json.loads(self.checkpoint.read_text())
            self.assertEqual(saved[0]["session_id"], "proc_running")
        finally:
            child.wait(timeout=2)

    def test_checkpoint_snapshot_and_replace_are_serialized(self):
        registry = pr.ProcessRegistry()
        registry._running["older"] = pr.ProcessSession(id="older", command="true")
        entered = threading.Event()
        release = threading.Event()
        real_write = __import__("utils").atomic_json_write
        calls = 0

        def blocking_write(path, entries):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                self.assertTrue(release.wait(2))
            real_write(path, entries)

        with patch("utils.atomic_json_write", side_effect=blocking_write):
            first = threading.Thread(target=registry._write_checkpoint)
            first.start()
            self.assertTrue(entered.wait(2))

            def add_newer():
                with registry._lock:
                    registry._running["newer"] = pr.ProcessSession(
                        id="newer", command="true"
                    )
                registry._write_checkpoint()

            second = threading.Thread(target=add_newer)
            second.start()
            self.assertTrue(second.is_alive())
            release.set()
            first.join(2)
            second.join(2)
        self.assertEqual(
            {row["session_id"] for row in json.loads(self.checkpoint.read_text())},
            {"older", "newer"},
        )

    def test_failed_terminal_checkpoint_keeps_running_evidence_and_does_not_publish(self):
        registry = pr.ProcessRegistry()
        session = pr.ProcessSession(
            id="proc_retry", command="true", notify_on_complete=True
        )
        registry._running[session.id] = session
        self.assertTrue(registry._write_checkpoint())
        session.exited = True
        with patch("utils.atomic_json_write", side_effect=OSError("disk full")):
            registry._move_to_finished(session)
        self.assertIn(session.id, registry._running)
        self.assertNotIn(session.id, registry._pending_terminal_entries)
        self.assertTrue(registry.completion_queue.empty())
        self.assertEqual(json.loads(self.checkpoint.read_text())[0]["session_id"], session.id)

    def test_failed_ack_checkpoint_retains_pending_evidence(self):
        registry = pr.ProcessRegistry()
        registry._pending_terminal_entries["proc_ack"] = {
            "session_id": "proc_ack", "terminal_event": {
                "type": "completion", "session_id": "proc_ack"
            }
        }
        self.assertTrue(registry._write_checkpoint())
        with patch("utils.atomic_json_write", side_effect=OSError("disk full")):
            registry.acknowledge_terminal_notification("proc_ack")
        self.assertIn("proc_ack", registry._pending_terminal_entries)
        self.assertEqual(json.loads(self.checkpoint.read_text())[0]["session_id"], "proc_ack")

    def test_consumed_evidence_clears_only_at_owning_turn_acceptance(self):
        registry = pr.ProcessRegistry()
        for sid, owner in (("mine", "session-a"), ("other", "session-b")):
            registry._pending_terminal_entries[sid] = {
                "session_id": sid,
                "terminal_event": {
                    "type": "completion", "session_id": sid,
                    "session_key": owner,
                },
            }
            registry._completion_consumed.add(sid)
        self.assertTrue(registry._write_checkpoint())

        self.assertTrue(
            registry.acknowledge_consumed_terminal_notifications("session-a")
        )
        self.assertNotIn("mine", registry._pending_terminal_entries)
        self.assertIn("other", registry._pending_terminal_entries)

    def test_consumed_acceptance_write_failure_retries_without_loss(self):
        registry = pr.ProcessRegistry()
        registry._pending_terminal_entries["mine"] = {
            "session_id": "mine",
            "terminal_event": {
                "type": "completion", "session_id": "mine",
                "session_key": "session-a",
            },
        }
        registry._completion_consumed.add("mine")
        self.assertTrue(registry._write_checkpoint())
        with patch("utils.atomic_json_write", side_effect=OSError("disk full")):
            self.assertFalse(
                registry.acknowledge_consumed_terminal_notifications("session-a")
            )
        self.assertIn("mine", registry._pending_terminal_entries)
        self.assertTrue(
            registry.acknowledge_consumed_terminal_notifications("session-a")
        )
        self.assertEqual(json.loads(self.checkpoint.read_text()), [])

    def test_failed_terminal_transition_retries_on_watcher_rail(self):
        registry = pr.ProcessRegistry()
        session = pr.ProcessSession(
            id="retry", command="true", session_key="session-a",
            notify_on_complete=True,
        )
        registry._running[session.id] = session
        self.assertTrue(registry._write_checkpoint())
        session.exited = True
        session.exit_code = 0
        with patch("utils.atomic_json_write", side_effect=OSError("disk full")):
            registry._move_to_finished(session)
        self.assertFalse(registry.completion_queue.qsize())
        self.assertTrue(registry.ensure_terminal_checkpoint(session.id))
        self.assertEqual(registry.completion_queue.get_nowait()["session_id"], "retry")

    def test_closed_session_evidence_is_retained_with_bounded_capacity_and_no_replay(self):
        registry = pr.ProcessRegistry()
        registry._pending_terminal_entries["closed"] = {
            "session_id": "closed",
            "terminal_event": {
                "type": "completion", "session_id": "closed",
                "session_key": "old-session",
            },
        }
        self.assertTrue(registry.mark_terminal_notification_undeliverable(
            "closed", "parent_session_closed"
        ))
        with patch.object(pr, "MAX_PENDING_TERMINAL_NOTIFICATIONS", 1):
            self.assertFalse(registry.reserve_notify_spawn())
        fresh = pr.ProcessRegistry()
        fresh.recover_from_checkpoint()
        self.assertTrue(fresh.completion_queue.empty())
        self.assertEqual(fresh.pending_watchers, [])
        saved = json.loads(self.checkpoint.read_text())
        self.assertTrue(saved[0]["terminal_event"]["delivery_terminal"])

    def test_notify_capacity_is_reserved_before_spawn(self):
        registry = pr.ProcessRegistry()
        with patch.object(pr, "MAX_PENDING_TERMINAL_NOTIFICATIONS", 1):
            self.assertTrue(registry.reserve_notify_spawn())
            self.assertFalse(registry.reserve_notify_spawn())
            self.assertEqual(registry._running, {})
            registry.release_notify_spawn()
            self.assertTrue(registry.reserve_notify_spawn())

    def test_only_live_notify_jobs_count_as_graceful_work(self):
        registry = pr.ProcessRegistry()
        registry._running["notify"] = pr.ProcessSession(
            id="notify", command="sleep", notify_on_complete=True)
        registry._running["daemon"] = pr.ProcessSession(
            id="daemon", command="serve", notify_on_complete=False)
        self.assertEqual(registry.active_notify_process_count(), 1)

    def test_durable_backlog_rejects_without_phantom_running_record(self):
        db = Path(self.tmp.name) / "state.db"
        with patch.object(ad, "_db_path", lambda: db), \
             patch.object(ad, "_MAX_DURABLE_PENDING", 1):
            with ad._transaction() as conn:
                now = time.time()
                conn.execute(
                    "INSERT INTO async_delegations "
                    "(delegation_id,origin_session,state,dispatched_at,updated_at,delivery_state) "
                    "VALUES ('pending','owner','completed',?,?,'pending')", (now, now))
            before = set(ad._records)
            result = ad.dispatch_async_delegation(
                goal="bounded", context=None, toolsets=None, role="subagent",
                model=None, session_key="owner", runner=lambda: {"summary": "never"},
            )
            self.assertEqual(result["status"], "rejected")
            self.assertEqual(set(ad._records), before)
            with ad._transaction() as conn:
                self.assertEqual(conn.execute(
                    "SELECT COUNT(*) FROM async_delegations"
                ).fetchone()[0], 1)

    def test_dispatch_persistence_error_leaves_no_phantom_record(self):
        before = set(ad._records)
        with patch.object(ad, "_persist_dispatch", side_effect=OSError("disk full")):
            result = ad.dispatch_async_delegation(
                goal="must persist", context=None, toolsets=None,
                role="subagent", model=None, session_key="owner",
                runner=lambda: {"summary": "must not run"},
            )
        self.assertEqual(result["status"], "rejected")
        self.assertIn("disk full", result["error"])
        self.assertEqual(set(ad._records), before)

    def test_capacity_admission_is_atomic_across_processes(self):
        db = Path(self.tmp.name) / "state.db"
        start = Path(self.tmp.name) / "start"
        code = r'''
import json, os, sys, time
from pathlib import Path
from tools import async_delegation as ad
ad._db_path = lambda: Path(sys.argv[1])
ad._MAX_DURABLE_PENDING = 1
while not Path(sys.argv[2]).exists(): time.sleep(.005)
record = {"delegation_id": sys.argv[3], "session_key": "owner",
          "dispatched_at": time.time(), "goal": "g"}
print(json.dumps(ad._persist_dispatch(record)))
'''
        env = {**os.environ, "PYTHONPATH": str(Path(__file__).parents[1])}
        procs = [subprocess.Popen(
            [sys.executable, "-c", code, str(db), str(start), f"d{i}"],
            cwd=Path(__file__).parents[1], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        ) for i in range(2)]
        start.touch()
        results = []
        for proc in procs:
            stdout, stderr = proc.communicate(timeout=20)
            self.assertEqual(proc.returncode, 0, stderr)
            results.append(json.loads(stdout.strip()))
        self.assertEqual(sorted(results), [False, True])
        conn = __import__("sqlite3").connect(db)
        try:
            self.assertEqual(conn.execute(
                "SELECT COUNT(*) FROM async_delegations"
            ).fetchone()[0], 1)
        finally:
            conn.close()

    def test_schema_cache_detects_database_replacement_at_same_path(self):
        db = Path(self.tmp.name) / "state.db"
        with patch.object(ad, "_db_path", lambda: db):
            ad._SCHEMA_INITIALIZED_PATHS.clear()
            conn = ad._connect()
            conn.close()
            old = db.with_suffix(".old")
            db.rename(old)
            replacement = __import__("sqlite3").connect(db)
            replacement.close()
            conn = ad._connect()
            try:
                self.assertEqual(conn.execute(
                    "SELECT name FROM sqlite_master WHERE name='async_delegations'"
                ).fetchone(), ("async_delegations",))
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
