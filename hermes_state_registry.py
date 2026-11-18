"""Compatibility module for outside plugins that still import ``hermes_state_registry``; removed in v0.27.0.

``acquire`` opens a ``hermes_state.SessionDB`` on the storage interface and ``release`` closes it; nothing is shared
between callers. The first ``acquire`` in a process emits the ``hermes_state`` stand-in's one deprecation warning,
unless another stand-in name has already emitted it. A process that loaded an older release's ``hermes_state`` before
an update (a long-running server importing this module lazily) gets that release's ``SessionDB``, without the warning.
"""

import threading as _threading
import warnings as _warnings
from pathlib import Path as _Path

import hermes_state as _hermes_state


_legacy_lock = _threading.Lock()
_legacy_shared = {}


def acquire(db_path=None):
    """A ``hermes_state.SessionDB`` on the store of ``db_path`` (a ``state.db``), or of the current profile."""
    first_warning = getattr(_hermes_state, "_first_warning", None)
    if first_warning is None:
        return _hermes_state.SessionDB(db_path)
    if first_warning():
        _warnings.warn(
            f"hermes_state_registry.acquire is deprecated and is removed in {_hermes_state._REMOVAL_RELEASE}; use "
            f"persistence_assembly.open_persistence(profile_scope()) instead ({_hermes_state._MIGRATION_GUIDE}).",
            DeprecationWarning, stacklevel=2)
    return _hermes_state._SessionDB(db_path)


def release(db) -> bool:
    """Close ``db``: ``True``, as for any ``SessionDB`` ``acquire`` lent, and ``False`` for None."""
    if db is None:
        return False
    db.close()
    return True


def release_or_close(db) -> None:
    """Close ``db``."""
    release(db)


def get_shared_session_db(db_path=None, *, defer_fts_initialization=False):
    """The retained gateway's shared compatibility handle."""
    path = _Path(db_path) if db_path is not None else _Path(_hermes_state._default_db_path())
    path = path.resolve()
    with _legacy_lock:
        shared = _legacy_shared.get(path)
        if shared is not None:
            shared[1] += 1
            return shared[0]
        db = _hermes_state._SessionDB(path, defer_fts_initialization=defer_fts_initialization)
        _legacy_shared[path] = [db, 1]
        return db


def release_shared_session_db(db) -> bool:
    """Release one retained gateway compatibility reference."""
    close = False
    with _legacy_lock:
        for path, shared in list(_legacy_shared.items()):
            if shared[0] is db:
                shared[1] -= 1
                close = shared[1] <= 0
                if close:
                    del _legacy_shared[path]
                break
        else:
            return False
    if close:
        db.close()
    return True


def close_shared_session_dbs() -> int:
    """Close every retained gateway compatibility handle."""
    with _legacy_lock:
        handles = [shared[0] for shared in _legacy_shared.values()]
        _legacy_shared.clear()
    for db in handles:
        db.close()
    return len(handles)
