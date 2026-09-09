"""Schema negotiation remains valid when MCP SDK drops extension fields."""
from types import SimpleNamespace
import pytest
from tools.computer_use.cua_backend import CuaDriverBackend

@pytest.mark.parametrize('claim,schema,expected', [
    (False, True, True), (True, False, True), (False, False, False),
])
def test_token_attachment_uses_explicit_tool_schema(claim, schema, expected):
    backend = CuaDriverBackend.__new__(CuaDriverBackend)
    backend._snapshot_tokens = {17: 's00000001:17'}
    backend._session = SimpleNamespace(
        supports_capability=lambda cap, tool=None: claim,
        supports_input_property=lambda tool, prop: schema and tool == 'click' and prop == 'element_token',
    )
    args = {'element_index': 17}
    backend._maybe_attach_element_token('click', args)
    assert ('element_token' in args) is expected
    if expected:
        assert args['element_token'] == 's00000001:17'


def test_schema_does_not_invent_missing_snapshot_token():
    backend = CuaDriverBackend.__new__(CuaDriverBackend)
    backend._snapshot_tokens = {}
    backend._session = SimpleNamespace(
        supports_capability=lambda *a, **kw: False,
        supports_input_property=lambda *a: True,
    )
    args = {'element_index': 17}
    backend._maybe_attach_element_token('click', args)
    assert args == {'element_index': 17}
