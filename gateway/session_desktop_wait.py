"""Private, release-driven continuations for a cron fire parked on the desktop.

The scheduler books the original occurrence once. A ready event admits a new
turn on that SAME frozen cron conversation, never cron.submit/run or a new job.
Canonical history is the checkpoint; the wait ledger contains no tool arguments.
"""
from functools import partial
import asyncio
import json
from pathlib import Path

from hermes_state_keys import CRON_PRINCIPAL_ID, cron_request_id
from hermes_state_runtime import RuntimeStoreError, admit_session_input, list_session_admissions

RESUME_PROMPT = (
    '[INTERNAL CONTINUATION: The shared desktop was released. Continue the original '
    'scheduled task from the saved conversation and previously blocked tool action. '
    'Recheck current application state before acting; do not repeat completed side '
    'effects or restart the task from its beginning. Respect later cancellation. '
    'Continue through verified completion, not a status acknowledgement.]'
)
EXPIRED_NOTICE = (
    '⚠️ The queued Computer Use request expired after waiting three hours for the '
    'shared desktop. The blocked desktop action was not executed. The normal '
    'schedule is unchanged; send a new request if this occurrence is still needed.'
)


def checkpoint_history(db, root_session_id, wait_id):
    """Resolve compression and require the literal blocked result in canonical history."""
    tip = db.get_compression_tip(root_session_id) or root_session_id
    row = db.get_session(tip)
    from gateway.run import _USER_BOUNDARY_END_REASONS
    if row is None or row.get('end_reason') in _USER_BOUNDARY_END_REASONS:
        raise RuntimeStoreError('not_found')
    history = db.get_messages_as_conversation(tip, include_ancestors=True)
    for message in reversed(history):
        if message.get('role') != 'tool' or message.get('tool_name') not in ('computer_use', 'terminal', 'execute_code'):
            continue
        try:
            result = json.loads(message.get('content') or '')
        except (ValueError, TypeError):
            continue
        if isinstance(result, dict) and result.get('code') == 'desktop_parked' and result.get('wait_id') == wait_id:
            return tip, history
    raise RuntimeStoreError('admission_conflict')


def _frozen_fire(authority, event):
    """Read the scheduler's durable receipt, never a currently edited job or ambient sender."""
    from gateway.session_local_recovery import owned_receipt
    from gateway.session_policy import restore_policy
    root = str(event.get('cron_session_id') or '')
    receipt, _, _, _ = owned_receipt(authority, root)
    if receipt['principal_id'] != CRON_PRINCIPAL_ID:
        raise RuntimeStoreError('permission_denied')
    policy = restore_policy(receipt['policy'])
    data = json.loads(policy.request_json)
    if (policy.source != 'cron' or data['cron_job']['id'] != event.get('cron_job_id')
            or data['request_id'] != cron_request_id(event.get('cron_job_id'), event.get('cron_execution_id'))):
        raise RuntimeStoreError('admission_conflict')
    rows = list_session_admissions(authority.db, session_id=root, pending_only=False)
    original = next((r for r in rows if r['request_id'] == data['request_id']), None)
    if original is None or (event.get('kind') != 'expired' and (
            original['status'] != 'terminal' or original['outcome'] != 'completed')):
        # No execution overlaps the parking turn. An interrupted/unknown fire
        # needs operator reconciliation, not an automatic replay of UI work.
        raise RuntimeStoreError('unknown_execution')
    return data, rows


async def deliver_cron_wait_for_runner(runner, event):
    """Only the gateway already owning this home may admit its continuation."""
    from gateway.session_cron import owner_for_home
    home = event.get('origin_hermes_home')
    owner = owner_for_home(home) if home else None
    if owner is None or owner[0].runner is not runner:
        return False
    from gateway.run import _profile_runtime_scope
    with _profile_runtime_scope(Path(home)):
        return await deliver_cron_wait(owner[0], event)


async def deliver_cron_wait(authority, event):
    """True only after an idempotent continuation/expiry handoff is committed."""
    from gateway.session_local_recovery import restore_local_session
    from gateway.session_runtime_workers import tracked_write
    from cron.delivery_queue import enqueue
    wait_id = str(event.get('wait_id') or '')
    if not wait_id:
        return False
    data = None
    try:
        authority._require_admission_open()
        data, rows = await asyncio.to_thread(_frozen_fire, authority, event)
        root = event['cron_session_id']
        request_id = 'desktop_wait:' + wait_id
        if event.get('kind') == 'ready' and any(r['request_id'] == request_id for r in rows):
            # Includes queued, started, terminal and unknown: never replay an
            # accepted UI continuation, even after an acknowledgement crash.
            return True
        if event.get('kind') == 'expired':
            await asyncio.to_thread(enqueue, wait_id + ':expired', data['cron_job'], EXPIRED_NOTICE)
            return True
        if event.get('kind') != 'ready':
            return False
        await asyncio.to_thread(checkpoint_history, authority.db, root, wait_id)
        ref = restore_local_session(authority, root)
        payload = {'text': RESUME_PROMPT, 'desktop_wait_v1': {'wait_id': wait_id}}
        await tracked_write(authority, partial(
            admit_session_input, authority.db, epoch=authority.epoch, principal_id=CRON_PRINCIPAL_ID,
            session_id=root, request_id=request_id, payload=payload,
            _authorize_write=authority._admission_gate()),
            then=partial(authority._admitted, ref, None), ordered=True,
            after=partial(authority._schedule_admitted, ref))
        return True
    except RuntimeStoreError as exc:
        if data is not None and exc.reason == 'not_found':
            from tools.computer_use.desktop_lease import discard_desktop_wait
            await asyncio.to_thread(discard_desktop_wait, wait_id)
            return True  # an explicit user boundary cancels, never strands the FIFO
        return False
    except (ValueError, KeyError):
        return False


async def execute_cron_wait(authority, ref, row, policy):
    """Execute one trusted continuation with saved history; queue final delivery once."""
    from gateway import session_cron
    from gateway.run import _profile_runtime_scope
    from agent.runtime_scope import bind_turn, claim_fence
    from cron.scheduler import run_job, _is_cron_silence_response
    from cron.delivery_queue import enqueue
    data = json.loads(policy.request_json)
    wait_id = (row['payload'].get('desktop_wait_v1') or {}).get('wait_id')
    if (not isinstance(wait_id, str) or not wait_id
            or row['request_id'] != 'desktop_wait:' + wait_id
            or row['principal_id'] != CRON_PRINCIPAL_ID
            or row['payload'] != {'text': RESUME_PROMPT, 'desktop_wait_v1': {'wait_id': wait_id}}):
        raise RuntimeStoreError('permission_denied')
    tip, _ = await asyncio.to_thread(checkpoint_history, authority.db, ref.session_id, wait_id)
    from tools.computer_use.desktop_lease import parked_desktop_wait
    import time
    waiting = await asyncio.to_thread(parked_desktop_wait, tip)
    if not waiting or waiting.get('wait_id') != wait_id or waiting['expires_at'] <= time.time():
        raise RuntimeStoreError('not_found')  # reset/cancel/expiry won after admission; no UI replay
    execution_id = session_cron.scheduler_execution_id(data['cron_job']['id'], data['request_id'])
    job = {k: v for k, v in data['cron_job'].items() if k not in ('fire_claim', 'run_claim', 'pending_slot')}
    token = session_cron.bind_execution(authority, ref.session_id, job['id'], execution_id)
    try:
        from cron.cancellation import CancelRequest
        home = (await asyncio.to_thread(Path(authority.db.db_path).resolve)).parent
        with _profile_runtime_scope(home), bind_turn(
                ref.session_id, row['admission_id'], claim_fence(row)):
            result = await asyncio.to_thread(session_cron._run_identified, run_job, job, execution_id,
                resume_wait_id=wait_id, cancel_event=CancelRequest(execution_id))
            content = result[2] if result[0] else 'Computer Use continuation failed: ' + str(result[3])
            if content and not _is_cron_silence_response(content):
                await asyncio.to_thread(enqueue, wait_id, job, content, for_failure=not result[0])
        authority.pending_results[row['admission_id']] = {
            'result': {'final_response': result[2], 'failed': not result[0], 'completed': result[0]}, 'usage': {}}
        if not result[0]:
            raise RuntimeError(result[3] or 'Desktop continuation failed')
        return result[2]
    finally:
        session_cron._execution.reset(token)
