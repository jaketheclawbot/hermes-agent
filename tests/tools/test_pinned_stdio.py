import hashlib
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
    with patch.object(backend, '_computer_use_cfg', return_value=cfg), patch.object(backend, 'resolve_cua_driver_cmd', return_value=cmd), patch.object(backend, 'cua_driver_pinned_status', return_value={'ready': ready, 'reason': 'mismatch'}), patch.object(backend, 'cua_driver_runtime_contract_status') as generic, patch.object(backend, '_maybe_repair_runtime_contract') as repair, patch.object(backend, '_maybe_nudge_update') as nudge, patch('tools.lazy_deps.ensure'):
        b = backend.CuaDriverBackend('unrestricted')
        with patch.object(b._session, 'start'), patch.object(b._session, 'call_tool'):
            if ready:
                b.start()
            else:
                with pytest.raises(RuntimeError, match='will not replace it automatically'):
                    b.start()
        generic.assert_not_called()
        repair.assert_not_called()
        nudge.assert_not_called()
