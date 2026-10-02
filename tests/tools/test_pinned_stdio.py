import hashlib
import json
from unittest.mock import patch
import pytest
from tools.computer_use.pinned_stdio import enabled
from tools.computer_use import cua_backend as backend

@pytest.fixture
def receipt(tmp_path):
    cmd = tmp_path / 'driver'
    cmd.write_text('approved')
    config = {'driver_version_pin': '0.7.1', 'pinned_stdio_transport': {
        'version': '0.7.1', 'command': str(cmd),
        'files': {str(cmd): hashlib.sha256(cmd.read_bytes()).hexdigest()}}}
    return config, str(cmd)

def test_opt_in_only():
    assert enabled({}, 'unrestricted', 'anything') is False

def test_exact_receipt(receipt):
    cfg, cmd = receipt
    assert enabled(cfg, 'unrestricted', cmd)


def test_receipt_command_is_default_resolver_target(receipt, monkeypatch):
    cfg, cmd = receipt
    from pathlib import Path

    Path(cmd).chmod(0o755)
    monkeypatch.delenv("HERMES_CUA_DRIVER_CMD", raising=False)
    with patch.object(backend, "_computer_use_cfg", return_value=cfg):
        assert backend.resolve_cua_driver_cmd() == cmd


@pytest.mark.parametrize('mode', ['standard', 'bounded'])
def test_never_drops_scoped_authorization(receipt, mode):
    cfg, cmd = receipt
    with pytest.raises(ValueError): enabled(cfg, mode, cmd)

def test_never_drops_manifest(receipt):
    cfg, cmd = receipt
    cfg['capability_manifest'] = 'ceiling.json'
    with pytest.raises(ValueError): enabled(cfg, 'unrestricted', cmd)

def test_changed_bytes_fail_closed(receipt):
    cfg, cmd = receipt
    from pathlib import Path
    Path(cmd).write_text('updated')
    with pytest.raises(ValueError): enabled(cfg, 'unrestricted', cmd)

def test_version_mismatch(receipt):
    cfg, cmd = receipt
    cfg['driver_version_pin'] = '0.20.0'
    with pytest.raises(ValueError): enabled(cfg, 'unrestricted', cmd)

def test_wrong_command(receipt):
    cfg, cmd = receipt
    with pytest.raises(ValueError): enabled(cfg, 'unrestricted', cmd + '-other')

def test_actual_backend_preserves_mode_without_modern_daemon(receipt):
    cfg, cmd = receipt
    with patch.object(backend, '_computer_use_cfg', return_value=cfg), patch.object(backend, 'resolve_cua_driver_cmd', return_value=cmd), patch.object(backend, '_EmbeddedCuaDaemon') as modern:
        b = backend.CuaDriverBackend('unrestricted')
        assert b.permission_mode == 'unrestricted'
        assert b._embedded_daemon is None
        modern.assert_not_called()

@pytest.mark.parametrize('ready', [True, False])
def test_pin_never_repairs_or_upgrades(receipt, ready):
    cfg, cmd = receipt
    with patch.object(backend, '_computer_use_cfg', return_value=cfg), patch.object(backend, 'resolve_cua_driver_cmd', return_value=cmd), patch.object(backend, 'cua_driver_pinned_status', return_value={'ready': ready, 'reason': 'mismatch'}), patch.object(backend, 'cua_driver_runtime_contract_status') as generic, patch('tools.lazy_deps.ensure'):
        b = backend.CuaDriverBackend('unrestricted')
        with patch.object(b._session, 'start'), patch.object(b._session, 'call_tool'):
            if ready:
                b.start()
            else:
                with pytest.raises(RuntimeError, match='will not replace it automatically'):
                    b.start()
        generic.assert_not_called()


def test_actual_tool_factory_carries_effective_bypass_mode_to_pinned_backend(receipt, monkeypatch):
    """Exercise tool -> mode resolver -> extracted backend with no native start."""
    from tools import approval
    from tools.computer_use import tool

    cfg, cmd = receipt
    monkeypatch.setenv('HERMES_COMPUTER_USE_BACKEND', 'cua')
    tool.reset_backend_for_tests()
    try:
        with patch.object(backend, '_computer_use_cfg', return_value=cfg), \
             patch.object(backend, 'resolve_cua_driver_cmd', return_value=cmd), \
             patch.object(approval, 'is_approval_bypass_active_for_session', return_value=True), \
             patch.object(backend.CuaDriverBackend, 'start', autospec=True) as start:
            selected = tool._get_backend('pinned-fixture-session')
        assert isinstance(selected, backend.CuaDriverBackend)
        assert selected.permission_mode == 'unrestricted'
        assert selected._pinned_stdio is True
        assert selected._embedded_daemon is None
        start.assert_called_once_with(selected)
    finally:
        tool.reset_backend_for_tests()


def test_actual_tool_path_fails_closed_without_effective_bypass(receipt, monkeypatch):
    from tools import approval
    from tools.computer_use import tool

    cfg, cmd = receipt
    monkeypatch.setenv('HERMES_COMPUTER_USE_BACKEND', 'cua')
    tool.reset_backend_for_tests()
    try:
        with patch.object(backend, '_computer_use_cfg', return_value=cfg), \
             patch.object(backend, 'resolve_cua_driver_cmd', return_value=cmd), \
             patch.object(approval, 'is_approval_bypass_active_for_session', return_value=False), \
             patch.object(backend.CuaDriverBackend, 'start', autospec=True) as start:
            result = json.loads(tool.handle_computer_use(
                {'action': 'capture'}, session_id='standard-fixture-session'))
        assert 'requires unrestricted authorization' in result['error']
        start.assert_not_called()
    finally:
        tool.reset_backend_for_tests()


def test_actual_resolution_rejects_poisoned_driver_override(receipt, monkeypatch):
    """A different executable from the environment cannot replace the receipt command."""
    from tools import approval
    from tools.computer_use import tool

    cfg, _ = receipt
    poisoned = receipt[0]['pinned_stdio_transport']['command'] + '-poisoned'
    from pathlib import Path
    Path(poisoned).write_text('#!/bin/sh\nexit 0\n')
    Path(poisoned).chmod(0o755)
    monkeypatch.setenv('HERMES_CUA_DRIVER_CMD', poisoned)
    monkeypatch.setenv('HERMES_COMPUTER_USE_BACKEND', 'cua')
    tool.reset_backend_for_tests()
    try:
        with patch.object(backend, '_computer_use_cfg', return_value=cfg), \
             patch.object(approval, 'is_approval_bypass_active_for_session', return_value=True), \
             patch.object(backend.CuaDriverBackend, 'start', autospec=True) as start:
            result = json.loads(tool.handle_computer_use(
                {'action': 'capture'}, session_id='poisoned-fixture-session'))
        assert 'does not match the approved receipt' in result['error']
        start.assert_not_called()
    finally:
        tool.reset_backend_for_tests()


def test_extracted_driver_keeps_receipt_command_for_manifest_invocation(receipt):
    from types import SimpleNamespace
    from tools.computer_use import cua_backend_driver as driver

    cfg, cmd = receipt
    manifest = json.dumps({'mcp_invocation': {'command': cmd, 'args': ['mcp']}})
    with patch.object(backend, '_computer_use_cfg', return_value=cfg), \
         patch.object(backend, '_cua_no_overlay', return_value=False), \
         patch.object(backend, '_run_driver', return_value=SimpleNamespace(
             stdout=manifest, stderr='', returncode=0)) as run:
        assert enabled(cfg, 'unrestricted', cmd)
        assert driver._resolve_mcp_invocation(cmd) == (cmd, ['mcp'])
    run.assert_called_once_with(cmd, 'manifest', timeout=6.0, swallow=Exception)


def test_extracted_session_blocks_cli_fallback_before_process_use(receipt):
    from tools.computer_use.cua_backend_session import _CuaDriverSession

    cfg, _ = receipt
    session = object.__new__(_CuaDriverSession)
    with patch.object(backend, '_computer_use_cfg', return_value=cfg), \
         patch('subprocess.run') as run:
        with pytest.raises(RuntimeError, match='cannot fall back'):
            session._call_tool_via_cli('click', {'x': 1, 'y': 2}, 1.0)
    run.assert_not_called()
