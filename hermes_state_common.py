"""A text marker for the update downgrade guard of v0.22.7..v0.23.1; this module holds no code.

Those updaters read the target tree's root ``hermes_state*.py`` for the runtime ledger table below and refuse the
update as a downgrade when no file carries it. The session store lives in ``persistence/sqlite/``; this file keeps
the text so a host on one of those releases can still update.
"""

# CREATE TABLE IF NOT EXISTS session_admissions

# Private compatibility names used by the retained gateway startup-maintenance contract.
from persistence.sqlite._schema_ddl import SCHEMA_VERSION, _FTS_TRIGGERS
