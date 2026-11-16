"""Parking stops after canonical tool results, before another model/compression call."""
import json
from types import SimpleNamespace


def test_desktop_parking_ends_tool_round_with_canonical_checkpoint(tmp_path, monkeypatch):
    from agent import turn_tool_round, prepared_compaction
    from tools.computer_use import desktop_lease, tool
    from tools.bot_desktop import runtime
    from hermes_state import SessionDB

    monkeypatch.setattr(desktop_lease, '_paths', lambda: (tmp_path / 'lease.json', tmp_path / 'lease.guard'))
    monkeypatch.setattr(runtime, 'ensure_started_for_tool', lambda: None)
    monkeypatch.setattr(tool, '_backend_for_call', lambda *a, **k: (_ for _ in ()).throw(AssertionError('UI started')))
    monkeypatch.setattr(prepared_compaction, 'maybe_prepare', lambda *a, **k: None)
    monkeypatch.setattr(turn_tool_round, 'validate_tool_calls', lambda *a, **k: SimpleNamespace(action='execute', mixed_invalid_batch=False))
    monkeypatch.setattr(turn_tool_round, 'compress_after_tool_results', lambda *a, **k: (_ for _ in ()).throw(AssertionError('compressed after parking')))
    call = SimpleNamespace(id='blocked', function=SimpleNamespace(name='computer_use', arguments='{"action":"capture"}'))
    staged = {'role': 'assistant', 'content': 'Resume review B; review A already done.', 'tool_calls': [
        {'id': 'blocked', 'type': 'function', 'function': {'name': 'computer_use', 'arguments': '{"action":"capture"}'}}]}
    monkeypatch.setattr(turn_tool_round, 'stage_tool_call_message', lambda *a, **k: (staged, False))
    desktop_lease.acquire_desktop('other-owner')
    db = SessionDB(tmp_path / 'state.db')
    db.create_session('waiter', source='cron')
    flushed = []

    def flush(messages, history):
        for message in messages[len(flushed):]:
            db.append_message('waiter', message['role'], message.get('content'),
                              tool_calls=message.get('tool_calls'), tool_call_id=message.get('tool_call_id'),
                              tool_name=message.get('name'))
            flushed.append(message)
        return True

    def execute(assistant, messages, *args):
        result = tool.handle_computer_use({'action': 'capture'}, session_id='waiter')
        messages.append({'role': 'tool', 'name': 'computer_use', 'tool_call_id': 'blocked', 'content': result})
        flush(messages, [])

    agent = SimpleNamespace(
        session_id='waiter', quiet_mode=True, verbose_logging=False, stream_delta_callback=None,
        valid_tool_names={'computer_use'}, _cap_delegate_task_calls=lambda calls: calls,
        _deduplicate_tool_calls=lambda calls: calls, _flush_messages_to_session_db=flush,
        _emit_interim_assistant_message=lambda message: None, _execute_tool_calls=execute,
    )
    try:
        verdict = turn_tool_round.run_tool_round(
            agent, assistant_message=SimpleNamespace(tool_calls=[call]), finish_reason='tool_calls', messages=[],
            conversation_history=[], api_call_count=1, effective_task_id='task', user_message='Finish the reviews',
            system_message=None, active_system_prompt='', compression_attempts=0, max_compression_attempts=2,
            final_response=None, failed=False, _turn_exit_reason='text', truncated_tool_call_retries=0,
            current_turn_user_idx=0)
        assert verdict.action == 'break'
        assert verdict._turn_exit_reason == 'desktop_parked'
        assert verdict.final_response == '[SILENT]'
        history = db.get_messages_as_conversation('waiter')
        assert history[0]['tool_calls'][0]['function']['arguments'] == '{"action":"capture"}'
        assert json.loads(history[1]['content'])['code'] == 'desktop_parked'
        assert desktop_lease.parked_desktop_wait('waiter')['wait_id'] == json.loads(history[1]['content'])['wait_id']
    finally:
        db.close()
