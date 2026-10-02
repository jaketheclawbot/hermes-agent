"""Detached Desktop/TUI turns use child-owned activity, not process heartbeats."""

from pathlib import Path
import sys
import threading
import time

import pytest

from tui_gateway import server
from tui_gateway.host_supervisor import HostSupervisor


_DEFAULT_TRANSPORT = object()
_CHILD_TURN_CEILING_S = 20.0
_TURN_SETTLE_SLACK_S = 2.0


class _Session(dict):
    """Expose the production ``running`` transition as an event for bounded waits."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.settled = threading.Event()

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if key == "running" and value is False:
            self.settled.set()


class _Timer:
    def __init__(self, delay, callback):
        self.delay, self.callback = delay, callback

    def start(self):
        pass

    def cancel(self):
        pass


def _session(sid, *, transport=_DEFAULT_TRANSPORT):
    if transport is _DEFAULT_TRANSPORT:
        transport = server._detached_ws_transport
    return _Session(agent=None, agent_ready=threading.Event(), session_key=sid,
                    history=[], history_version=0, history_lock=threading.Lock(),
                    running=True, transport=transport,
                    attached_images=[], cols=80, source="desktop", inflight_turn=None)


@pytest.mark.parametrize("change", ["none", "other-session", "old-turn", "not-running", "stale", "missing"])
def test_activity_relay_is_fenced_and_ages(monkeypatch, change):
    session = _session("session")
    session.update(_compute_host_active=True, _compute_host_turn_id="new-turn")
    monkeypatch.setattr(server, "_sessions", {"session": session})
    monkeypatch.setattr(server, "_WS_ORPHAN_ACTIVITY_STALE_S", 30)
    monkeypatch.setattr(server, "write_json", lambda msg: pytest.fail("internal activity leaked to client"))
    params: dict = dict(session_id="session", turn_id="new-turn", activity_ns=time.perf_counter_ns())
    if change == "other-session":
        params["session_id"] = "other"
    elif change == "old-turn":
        params["turn_id"] = "old-turn"
    elif change == "not-running":
        session["running"] = False
    elif change == "stale":
        params["activity_ns"] -= 31_000_000_000
    elif change == "missing":
        params["activity_ns"] = None
    server._relay_compute_host_rpc({"jsonrpc": "2.0", "method": "compute_host.activity", "params": params})
    assert server._ws_orphan_turn_activity_is_fresh(session) is (change == "none")
    if change == "none":
        # Repeated delivery is an observation of the same clock, not a refresh.
        monkeypatch.setattr(server.time, "perf_counter_ns", lambda: params["activity_ns"] + 31_000_000_000)
        server._relay_compute_host_rpc({"method": "compute_host.activity", "params": params})
        assert not server._ws_orphan_turn_activity_is_fresh(session)
