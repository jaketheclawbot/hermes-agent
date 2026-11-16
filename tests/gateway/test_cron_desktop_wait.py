"""Release-driven cron continuation: real admissions/history, no UI or model calls."""
import asyncio
import json
from types import SimpleNamespace

import pytest

from tools.computer_use import desktop_lease


@pytest.fixture
def cron_owner(tmp_path, monkeypatch):
    from cron import jobs
    from gateway import run, session_cron
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import SessionAuthority
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(desktop_lease, '_paths', lambda: (tmp_path / 'lease.json', tmp_path / 'lease.guard'))
    job = {'id': 'reviews', 'prompt': 'Reply only to unreplied reviews; preserve completed replies.',
           'model': 'job-model', 'schedule': {'kind': 'cron', 'expr': '0 10 * * *'},
           'timezone': 'Europe/Paris', 'deliver': 'discord:parent:thread'}
    monkeypatch.setattr(jobs, 'get_job', lambda jid: dict(job) if jid == job['id'] else None)
    monkeypatch.setattr(run, '_load_gateway_config', lambda: {'model': {}, 'platform_toolsets': {'cli': []}})
    db = SessionDB(tmp_path / 'state.db')
    runner = SimpleNamespace(_draining=False, adapters={},
                             session_store=SessionStore(tmp_path / 'sessions', GatewayConfig()))
    authority = SessionAuthority(runner, profile_id=str(tmp_path), instance_id='first', db=db,
                                 epoch=begin_runtime_epoch(db, instance_id='first'))
    runner.session_authority = authority
    monkeypatch.setattr(authority, '_schedule', lambda ref: None)
    yield authority, job
    db.close()


async def park_fire(authority, job):
    from gateway import session_cron
    from hermes_state_runtime import claim_session_input, settle_session_input

    receipt = await session_cron.operation(authority, 'submit', {
        'job_id': job['id'], 'request_id': 'original-fire', 'extra_prompt': None})
    sid = receipt['session_id']
    row = claim_session_input(authority.db, epoch=authority.epoch, session_id=sid)
    authority.db.append_message(sid, 'user', job['prompt'])
    authority.db.append_message(sid, 'assistant', 'Already replied to review A. Next: review B.', tool_calls=[{
        'id': 'blocked', 'type': 'function', 'function': {
            'name': 'computer_use', 'arguments': '{"action":"click","element":42}'}}])
    desktop_lease.acquire_desktop('other-owner')
    token = session_cron.bind_execution(authority, sid, job['id'], 'original-fire')
    try:
        parked = desktop_lease.acquire_desktop(sid, park=True)
    finally:
        session_cron._execution.reset(token)
    authority.db.append_message(sid, 'tool', json.dumps(parked), tool_call_id='blocked', tool_name='computer_use')
    authority.db.append_message(sid, 'assistant', '[SILENT]')
    authority.db.end_session(sid, 'cron_complete')  # even an already completed cron retains this exact checkpoint
    settle_session_input(authority.db, epoch=authority.epoch, admission_id=row['admission_id'],
                         generation=row['generation'], outcome='completed')
    return receipt, parked


@pytest.mark.asyncio
async def test_cron_wait_captures_fresh_fire_identity_not_ambient_sender(cron_owner, monkeypatch):
    from gateway.session_context import set_session_vars, clear_session_vars
    authority, job = cron_owner
    tokens = set_session_vars(platform='discord', chat_id='unrelated', session_key='agent:main:discord:dm:unrelated')
    try:
        receipt, parked = await park_fire(authority, job)
    finally:
        clear_session_vars(tokens)
    desktop_lease.release_desktop('other-owner')
    event = desktop_lease.claim_desktop_wait_events('gateway')[0]
    assert event['cron_session_id'] == receipt['session_id']
    assert event['cron_execution_id'] == 'original-fire'
    assert event['cron_job_id'] == 'reviews'
    assert event.get('chat_id') != 'unrelated'
    assert event['wait_id'] == parked['wait_id']
    # A genuine new parked turn is not mislabeled as a completed cron task.
    from cron.scheduler import _finalize_cron_session
    from hermes_state import SessionDB
    sid = receipt['session_id']
    authority.db.reopen_session(sid)
    _finalize_cron_session(SessionDB(authority.db.db_path), None, job['id'], 'reviews', sid)
    assert authority.db.get_session(sid)['end_reason'] == 'desktop_parked'


@pytest.mark.asyncio
async def test_completed_cron_wakes_once_with_exact_checkpoint_and_frozen_delivery(cron_owner, monkeypatch):
    from gateway import session_desktop_wait
    from hermes_state_runtime import claim_session_input, list_session_admissions
    from cron import scheduler, delivery_queue
    authority, job = cron_owner
    receipt, parked = await park_fire(authority, job)
    sid = receipt['session_id']
    job['deliver'] = 'discord:unrelated-channel'
    job['model'] = 'later-job-edit'
    desktop_lease.release_desktop('other-owner')
    event = desktop_lease.claim_desktop_wait_events('gateway')[0]
    session_desktop_wait._frozen_fire(authority, event)
    _, checkpoint = session_desktop_wait.checkpoint_history(authority.db, sid, parked['wait_id'])
    assert checkpoint[0]['content'] == job['prompt']
    assert json.loads(checkpoint[1]['tool_calls'][0]['function']['arguments']) == {'action': 'click', 'element': 42}
    assert await session_desktop_wait.deliver_cron_wait(authority, event) is True
    assert await session_desktop_wait.deliver_cron_wait(authority, event) is True  # lost ACK/restart replay
    admissions = list_session_admissions(authority.db, session_id=sid, pending_only=False)
    assert len(admissions) == 2
    assert admissions[0]['request_id'] == 'cron:reviews:original-fire'
    resumed = claim_session_input(authority.db, epoch=authority.epoch, session_id=sid)
    seen = []

    def run_job(frozen, **kwargs):
        seen.append((dict(frozen), dict(kwargs)))
        assert kwargs['resume_wait_id'] == parked['wait_id']
        assert frozen['prompt'] == job['prompt']
        assert desktop_lease.acquire_desktop(sid)['ok']
        return True, 'doc', 'Review B replied; review A left unchanged.', None

    monkeypatch.setattr(scheduler, 'run_job', run_job)
    policy = authority.runner.adapters[next(iter(authority.runner.adapters))].policies[sid]
    from gateway.session_contract import SessionRef
    answer = await session_desktop_wait.execute_cron_wait(authority, SessionRef(authority.profile_id, sid), resumed, policy)
    assert answer == 'Review B replied; review A left unchanged.'
    assert len(seen) == 1
    assert seen[0][0]['deliver'] == 'discord:parent:thread'
    assert seen[0][0]['model'] == 'job-model'
    assert seen[0][0]['schedule'] == job['schedule']
    queued = delivery_queue.get_status(parked['wait_id'])
    assert queued['status'] == 'pending'
    assert json.loads(queued['job_json'])['deliver'] == 'discord:parent:thread'
    assert queued['content'] == answer
    assert len(list_session_admissions(authority.db, session_id=sid, pending_only=False)) == 2


@pytest.mark.asyncio
async def test_cron_wait_restart_reclaims_same_admission_without_another_fire(cron_owner):
    from gateway import session_desktop_wait
    from hermes_state_runtime import begin_runtime_epoch, list_session_admissions
    authority, job = cron_owner
    receipt, _ = await park_fire(authority, job)
    desktop_lease.release_desktop('other-owner')
    event = desktop_lease.claim_desktop_wait_events('old-gateway')[0]
    assert await session_desktop_wait.deliver_cron_wait(authority, event)
    authority.epoch = begin_runtime_epoch(authority.db, instance_id='replacement')
    event['claim_id'] = 'new-claim-after-crash'
    assert await session_desktop_wait.deliver_cron_wait(authority, event)
    rows = list_session_admissions(authority.db, session_id=receipt['session_id'], pending_only=False)
    assert len(rows) == 2
    assert rows[1]['status'] == 'queued'


@pytest.mark.asyncio
async def test_cron_wait_rejects_missing_checkpoint_closed_user_and_wrong_fire(cron_owner):
    from gateway import session_desktop_wait
    authority, job = cron_owner
    receipt, _ = await park_fire(authority, job)
    desktop_lease.release_desktop('other-owner')
    event = desktop_lease.claim_desktop_wait_events('gateway')[0]
    assert await session_desktop_wait.deliver_cron_wait(authority, dict(event, cron_execution_id='foreign-fire')) is False
    assert await session_desktop_wait.deliver_cron_wait(authority, dict(event, wait_id='not-recorded')) is False
    authority.db.reopen_session(receipt['session_id'])
    authority.db.end_session(receipt['session_id'], 'session_reset')
    assert await session_desktop_wait.deliver_cron_wait(authority, event) is True
    assert desktop_lease.parked_desktop_wait(receipt['session_id']) is None


@pytest.mark.asyncio
async def test_expired_cron_wait_queues_notice_to_frozen_target_without_model(cron_owner):
    from gateway import session_desktop_wait
    from cron import delivery_queue
    authority, job = cron_owner
    _, parked = await park_fire(authority, job)
    event = desktop_lease.claim_desktop_wait_events('gateway', now=10**12)[0]
    assert event['kind'] == 'expired'
    assert await session_desktop_wait.deliver_cron_wait(authority, event) is True
    status = delivery_queue.get_status(parked['wait_id'] + ':expired')
    assert status['status'] == 'pending'


@pytest.mark.asyncio
async def test_real_watcher_bypasses_terminal_cron_classifier(cron_owner, monkeypatch):
    from gateway import session_cron
    from gateway.run import GatewayRunner
    from hermes_state_runtime import list_session_admissions
    authority, job = cron_owner
    receipt, parked = await park_fire(authority, job)
    runner = authority.runner
    runner._running = True
    session_cron.bind_owner(authority)
    original_complete = desktop_lease.complete_desktop_wait_event

    def complete(*args, **kwargs):
        runner._running = False
        return original_complete(*args, **kwargs)

    monkeypatch.setattr(desktop_lease, 'complete_desktop_wait_event', complete)
    # No classifier or generic notification method is provided: using either is a bug.
    desktop_lease.release_desktop('other-owner')
    try:
        await GatewayRunner._desktop_wait_watcher(runner, interval=0)
    finally:
        session_cron.unbind_owner(authority)
    rows = list_session_admissions(authority.db, session_id=receipt['session_id'], pending_only=False)
    assert len(rows) == 2
    assert rows[1]['request_id'] == 'desktop_wait:' + parked['wait_id']
    assert desktop_lease.parked_desktop_wait(receipt['session_id'])['event_delivered_at']


@pytest.mark.asyncio
async def test_run_job_loads_exact_history_and_does_not_rerun_prerun_script(cron_owner, monkeypatch):
    from cron import scheduler
    from gateway import session_cron
    from gateway.session_desktop_wait import RESUME_PROMPT
    from hermes_state import SessionDB
    authority, job = cron_owner
    receipt, parked = await park_fire(authority, job)
    sid = receipt['session_id']
    desktop_lease.release_desktop('other-owner')
    seen = []

    class Agent:
        def __init__(self, session_id, session_db):
            self.session_id, self.db = session_id, session_db

        def run_conversation(self, prompt, *, task_id, conversation_history):
            seen.append(conversation_history)
            assert self.session_id == sid
            assert prompt == RESUME_PROMPT
            assert conversation_history[0]['content'] == job['prompt']
            assert 'Already replied to review A' in conversation_history[1]['content']
            assert json.loads(conversation_history[2]['content'])['wait_id'] == parked['wait_id']
            assert session_cron.current_execution()[3] == 'original-fire'
            assert desktop_lease.acquire_desktop(sid)['ok']
            self.db.append_message(sid, 'user', prompt)
            self.db.append_message(sid, 'assistant', 'Completed review B, without repeating A.')
            desktop_lease.release_desktop(sid)
            return {'final_response': 'Completed review B, without repeating A.', 'completed': True, 'messages': []}

    monkeypatch.setattr(scheduler, '_prepare_job_prompt', lambda *a, **k: pytest.fail('re-ran prompt/script'))
    monkeypatch.setattr(scheduler, '_reload_dotenv_and_publish_delivery_target', lambda *a: None)
    monkeypatch.setattr(scheduler, '_load_cron_job_config', lambda *a: SimpleNamespace(cfg={}, model='job-model'))
    monkeypatch.setattr(scheduler, '_resolve_cron_agent_setup', lambda *a: scheduler._CronAgentSetup(model='job-model'))
    monkeypatch.setattr(scheduler, '_open_cron_session_db', lambda *a: SessionDB(authority.db.db_path))
    monkeypatch.setattr(scheduler, '_construct_cron_agent', lambda *a, **k: Agent(k['session_id'], k['session_db']))
    token = session_cron.bind_execution(authority, sid, job['id'], 'original-fire')
    try:
        result = await asyncio.to_thread(scheduler.run_job, job, execution_id='original-fire',
                                        resume_wait_id=parked['wait_id'], defer_agent_teardown=[])
    finally:
        session_cron._execution.reset(token)
    assert result[0] is True, result[3]
    assert result[2] == 'Completed review B, without repeating A.'
    assert len(seen) == 1
    assert authority.db.get_session(sid)['end_reason'] == 'cron_complete'
    assert desktop_lease.parked_desktop_wait(sid) is None


def test_continuation_without_original_owner_never_submits_a_new_fire(monkeypatch):
    from cron import scheduler, scheduler_authority
    monkeypatch.setattr(scheduler_authority, 'run_canonical_job', lambda *a, **k: pytest.fail('new fire submitted'))
    with pytest.raises(RuntimeError, match='original cron owner'):
        scheduler.run_job({'id': 'reviews'}, execution_id='foreign-fire', resume_wait_id='old-wait')


@pytest.mark.asyncio
async def test_queued_continuation_cancellation_never_enters_model(cron_owner, monkeypatch):
    from gateway import session_desktop_wait
    from gateway.session_contract import SessionRef
    from cron import scheduler
    from hermes_state_runtime import claim_session_input, RuntimeStoreError
    authority, job = cron_owner
    receipt, _ = await park_fire(authority, job)
    sid = receipt['session_id']
    desktop_lease.release_desktop('other-owner')
    event = desktop_lease.claim_desktop_wait_events('gateway')[0]
    assert await session_desktop_wait.deliver_cron_wait(authority, event)
    row = claim_session_input(authority.db, epoch=authority.epoch, session_id=sid)
    desktop_lease.cancel_desktop_wait(sid)
    monkeypatch.setattr(scheduler, 'run_job', lambda *a, **k: pytest.fail('stale continuation executed'))
    policy = next(iter(authority.runner.adapters.values())).policies[sid]
    with pytest.raises(RuntimeStoreError):
        await session_desktop_wait.execute_cron_wait(authority, SessionRef(authority.profile_id, sid), row, policy)
