"""Public regression coverage for evidence-bounded cron reports."""
from cron.failure_report import plain_failure_reason, scheduler_report, repair_summary


def test_traceback_tail_is_used_before_shortening():
    error = 'Traceback (most recent call last):\n' + ('  File "worker.py", line 1\n' * 30)
    error += 'ValueError: the configured input is missing'
    assert plain_failure_reason(error) == 'the configured input is missing.'


def test_scheduler_notice_does_not_invent_recovery():
    report = scheduler_report({'id': 'example', 'name': 'Example'}, 'ValueError: missing input')
    assert 'Evidence unavailable' in report
    assert 'automatically retry' not in report
    assert 'reconciler has not processed' not in report


def test_local_limit_is_not_reported_as_provider_credit_exhaustion():
    from cron.failure_report import triage_summary
    report = triage_summary({'category': 'technical', 'provider_error': 'JEV daily call/spend ceiling'})
    assert 'JEV was not called' in report
    assert 'local daily safety limit' in report
    assert 'not a JEV credit-balance error' in report


def test_older_attempts_are_labelled_as_incident_history():
    report, _, _ = repair_summary(
        {'action': 'bounded_escalation', 'reason': 'Attempt limit reached'},
        {'event': 'new', 'attempts_event': 'old', 'attempts': [{'status': 'failed'}]},
    )
    assert 'including earlier runs' in report
    assert 'no concise action/outcome details' in report
