"""Evidence-bounded, human-readable cron failure reports.

This module only renders evidence supplied by the operation being reported.  It deliberately
does not read triage ledgers or repair packets: callers must first prove that those records belong
to the current operation, which prevents an old classification or repair attempt being described
as current.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Mapping, Optional


_SECRET = re.compile(
    r"(?i)(bearer|token|api[_ -]?key|password|secret)\s*[:=]?\s*[^\s,;]+"
)
_SECRET_VALUE = re.compile(r"(?i)\b(?:sk-|ghp_|xoxb-|apikey_)[A-Za-z0-9_-]+")
_URL_CREDENTIAL = re.compile(r"(?i)(https?://)[^/@\s:]+:[^/@\s]+@")
_URL_QUERY = re.compile(r"(https?://[^\s?#]+)[?#][^\s]+")
_MENTION = re.compile(r"<@([!&]?)(\d+)>")
_CAPACITY_STATUSES = frozenset({
    "waiting_capacity", "retention_capacity", "test_runtime_capacity",
    "specialist_correction_pending",
})


def safe_text(value: Any, limit: int = 500) -> str:
    """Bound untrusted diagnostics and neutralize common secrets/Discord mentions."""
    text = _sanitized_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _sanitized_text(value: Any) -> str:
    """Sanitize complete evidence before any detection or shortening."""
    text = " ".join(str(value or "").split())
    text = _SECRET.sub(r"\1=[REDACTED]", text)
    text = _SECRET_VALUE.sub("[REDACTED]", text)
    text = _URL_CREDENTIAL.sub(r"\1[REDACTED]@", text)
    text = _URL_QUERY.sub(r"\1?[REDACTED]", text)
    text = text.replace("@everyone", "@ everyone").replace("@here", "@ here")
    text = _MENTION.sub(lambda match: f"<@\u200b{match[1]}{match[2]}>", text)
    return text


def bounded_evidence(value: Any, limit: int = 500) -> str:
    """Keep both traceback context and its decisive tail within a fixed diagnostic bound."""
    text = _sanitized_text(value)
    if len(text) <= limit:
        return text
    marker = " ... [middle omitted] ... "
    available = max(2, limit - len(marker))
    head = available // 3
    return text[:head].rstrip() + marker + text[-(available - head):].lstrip()


def current_operation_evidence(
    operation_stamp: Any, evidence_stamp: Any,
) -> bool:
    """Exact operation identity gate used before attaching triage/repair evidence."""
    return bool(operation_stamp is not None and str(operation_stamp) == str(evidence_stamp))


def newer_operation(current_stamp: Any, historical_stamp: Any) -> bool:
    """Whether a scheduler observation is newer than a detached historical receipt."""
    if current_stamp is None:
        return False
    if historical_stamp is None:
        return True
    try:
        current = datetime.fromisoformat(str(current_stamp).replace("Z", "+00:00"))
        historical = datetime.fromisoformat(str(historical_stamp).replace("Z", "+00:00"))
        return current > historical
    except (TypeError, ValueError):
        return False


def plain_failure_reason(error: Any, fallback: Optional[str] = None) -> str:
    """Short cause led by the concrete failure, with a special case for the updater launcher."""
    complete = _sanitized_text(error)
    lower = complete.lower()
    if "launchctl" in lower and "print" in lower and (
        "exit status 113" in lower or "returned non-zero exit status 113" in lower
    ):
        return (
            "The macOS updater launcher could not inspect its launch service (launchctl exit "
            "113), so it stopped before the updater started."
        )
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    if "traceback" in lower:
        for line in reversed(lines):
            if line.startswith(("Traceback", "File ", "^")):
                continue
            cleaned = re.sub(r"^(?:[\w.]+\.)?([A-Z]\w*(?:Error|Exception)):\s*", "", line)
            if cleaned != line:
                return safe_text(cleaned, 260).rstrip(".") + "."
    if fallback:
        notice = _sanitized_text(fallback)
        for prefix in (" failed: ", " did not run: "):
            if prefix in notice:
                notice = notice.split(prefix, 1)[1]
                break
        for marker in (
            ". See the full run", ". Run log:", ". No backup provider",
            ". It will run again", ". Sign in again", ". Top up or wait",
            ". Pick another model", ". Check what it was doing", ". No model was invoked",
            ". Nothing was charged", ". Run it again with",
        ):
            if marker in notice:
                notice = notice.split(marker, 1)[0]
        if notice:
            return safe_text(notice.rstrip("."), 260) + "."
    if not complete:
        return "No diagnostic was recorded."
    for line in reversed(lines):
        if line.startswith(("Traceback", "File ", "^")):
            continue
        cleaned = re.sub(r"^(?:[\w.]+\.)?([A-Z]\w*(?:Error|Exception)):\s*", "", line)
        if cleaned != line or line.lower().startswith(("error:", "failed:")):
            return safe_text(cleaned, 260).rstrip(".") + "."
    return safe_text(complete, 260).rstrip(".") + "."


def _impact(job: Mapping[str, Any], error: Any) -> str:
    lower = _sanitized_text(error).lower()
    if job.get("no_agent") and "launchctl" in lower and "exit status 113" in lower:
        return "The launcher failed before the updater worker started; no recovery is evidenced here."
    return "This operation stopped before completion; downstream impact is not recorded here."


def scheduler_report(
    job: Mapping[str, Any], error: Any, *, cause: Optional[str] = None,
    guidance: Optional[str] = None,
) -> str:
    """Report for immediate scheduler delivery, before the reconciler has produced evidence."""
    name = safe_text(job.get("name") or job.get("id") or "cron job", 120)
    job_id = safe_text(job.get("id") or name, 120)
    why = plain_failure_reason(error, cause)
    detail = bounded_evidence(error, 420) or "not recorded"
    next_step = f"Inspect the saved diagnostics with `hermes cron runs {job_id}`."
    if guidance:
        next_step += " " + safe_text(guidance, 360)
    if cause and "rejected the sign-in" in cause:
        commands = re.findall(r"`([^`]+)`", cause)
        if commands:
            next_step += f" If authentication is still invalid, use `{commands[0]}`."
    return (
        f"⚠️ Cron '{name}' failed.\n"
        f"**Why:** {why}\n"
        f"**Impact:** {_impact(job, error)}\n"
        "**JEV category:** Evidence unavailable in this scheduler notice; no category asserted.\n"
        "**Repair:** Evidence unavailable in this scheduler notice; no repair activity asserted.\n"
        f"**Next:** {next_step}\n"
        "**Your action:** Whether action is required is not determined by this scheduler notice.\n"
        f"**Technical:** {detail}"
    )


def triage_summary(decision: Mapping[str, Any]) -> str:
    """Describe only the classifier evidence attached to the current decision."""
    category = safe_text(decision.get("category") or "technical", 40)
    jev = decision.get("jev") if isinstance(decision.get("jev"), Mapping) else None
    if jev:
        confidence = jev.get("confidence")
        confidence_text = (
            f" ({float(confidence):.0%} confidence)"
            if isinstance(confidence, (int, float)) and not isinstance(confidence, bool)
            else ""
        )
        if jev.get("source") == "jev_low_confidence":
            return (
                f"JEV was low-confidence{confidence_text}; the deterministic fallback used "
                f"'{category}'."
            )
        return f"JEV categorized this operation as '{category}'{confidence_text}."
    if decision.get("provider_error"):
        return (
            f"JEV was unavailable ({safe_text(decision['provider_error'], 120)}); the "
            f"deterministic fallback used '{category}'."
        )
    return "No JEV or fallback source is recorded; category evidence is unavailable."


def _attempt_description(packet: Mapping[str, Any]) -> tuple[int, str]:
    attempts = packet.get("attempts")
    if not isinstance(attempts, list):
        return 0, ""
    records = [attempt for attempt in attempts if isinstance(attempt, Mapping)]
    details = []
    for index, attempt in enumerate(records, 1):
        action = attempt.get("action") or attempt.get("attempted") or attempt.get("summary")
        outcome = (
            attempt.get("outcome") or attempt.get("result") or attempt.get("reason")
            or attempt.get("error_type") or attempt.get("validation_error")
        )
        if action or outcome:
            parts = []
            if action:
                parts.append("action " + safe_text(action, 100))
            if outcome:
                parts.append("outcome " + safe_text(outcome, 120))
            details.append(f"attempt {index}: " + "; ".join(parts))
    return len(records), "; ".join(details)


def repair_summary(
    decision: Mapping[str, Any], packet: Optional[Mapping[str, Any]] = None,
) -> tuple[str, str, str]:
    """Return repair, next-owner, and operator-action lines from current-operation evidence."""
    packet = packet if isinstance(packet, Mapping) else {}
    if decision.get("repair_evidence_current") is False:
        return (
            "No repair attempt is recorded for this operation; an older episode packet was "
            "excluded.",
            "The reconciler owns a fresh, operation-scoped repair decision.",
            "No action now unless a later report records a verified human-only gate.",
        )
    status = safe_text(decision.get("repair_status") or packet.get("status"), 80)
    action = decision.get("action")
    reason = safe_text(decision.get("reason") or packet.get("reason"), 260)
    attempts, attempt_details = _attempt_description(packet)
    attempt_text = f" after {attempts} bounded attempt{'s' if attempts != 1 else ''}"
    if packet.get("attempts_event") and packet.get("attempts_event") != packet.get("event"):
        attempt_text += " across this unresolved incident, including earlier runs"
    if attempts:
        attempt_text += (
            f"; {attempt_details}" if attempt_details
            else "; no concise action/outcome details were recorded"
        )

    if action == "human_escalation" and decision.get("verified_human_gate") is True:
        owner = safe_text(decision.get("next_owner") or packet.get("next_owner") or "the operator", 100)
        return (
            f"Repair stopped at a verified human-only gate{attempt_text}: {reason or status}.",
            f"{owner} must complete the verified gate before automation can continue.",
            "Action required: complete the verified gate described above.",
        )
    if status in _CAPACITY_STATUSES:
        return (
            f"Repair is waiting for automation capacity{attempt_text}; evidence and remaining "
            "attempts were retained.",
            reason or "The recurring reconciler will continue when capacity is available.",
            "No action now; this is a capacity wait, not a human approval gate.",
        )
    if status == "runtime_activation_pending":
        if (packet.get("route") or {}).get("kind") == "updater_launcher_recovery":
            return (
                "The scoped recovery stopped at the native-service activation boundary; no "
                "source repair or service action was performed.",
                reason or "The reviewed external activation owner must validate and load the dormant service.",
                "A human operator must review the existing external activation helper; the gateway must not load or submit the service.",
            )
        owner = safe_text(packet.get("next_owner") or "the existing task owner", 100)
        return (
            f"Repair prepared and tested a candidate{attempt_text}, but this operation was not "
            "authorized to install it or restart the runtime.",
            f"{owner} owns the separately authorized activation step.",
            "No action unless you are the named activation owner.",
        )
    if status in {"investigation_unresolved", "infrastructure_unresolved"} or action == "bounded_escalation":
        return (
            f"Repair stopped unresolved{attempt_text}: {reason or 'the bounded attempt budget ended'}.",
            "The automation owner must review the retained diagnostics and choose a new scoped step.",
            "No approval is implied; the automation owner must arrange the next step.",
        )
    if action == "leave_wait" and decision.get("owner"):
        return (
            "No new repair agent was spawned; a verified active executor already owns this operation.",
            reason or "That executor remains the next owner.",
            "No action now.",
        )
    if action == "leave_wait":
        return (
            "No repair agent was spawned; triage selected a bounded wait for the original operation.",
            reason or "The reconciler will check the next original operation.",
            "No action now; this is an automation wait, not a human gate.",
        )
    if status in {"pending", "adopted"} or action == "llm_repair":
        return (
            "A repair investigation was queued, but no completed attempt is recorded yet."
            if attempts == 0 else f"The repair agent has{attempt_text}.",
            reason or "The repair supervisor owns the next bounded step.",
            "No action now.",
        )
    return (
        "No repair attempt is recorded for this operation.",
        reason or "The reconciler owns the next triage step.",
        "No action now.",
    )


def reconciler_report(
    job: Mapping[str, Any], error: Any, decision: Optional[Mapping[str, Any]] = None,
    packet: Optional[Mapping[str, Any]] = None,
) -> str:
    """Structured incident body after current-operation evidence has been identity-checked."""
    if decision is None:
        category = "Triage pending — no category is recorded for this operation yet."
        repair = "No repair activity is recorded for this operation."
        next_step = "The reconciler gap must be reviewed against this current operation."
        julian = "No action now unless a verified human-only gate is later recorded."
    else:
        category = triage_summary(decision)
        repair, next_step, julian = repair_summary(decision, packet)
    return (
        f"**Why:** {plain_failure_reason(error)}\n"
        f"**Impact:** {_impact(job, error)}\n"
        f"**JEV category:** {category}\n"
        f"**Repair:** {repair}\n"
        f"**Next:** {next_step}\n"
        f"**Your action:** {julian}\n"
        f"**Technical:** {bounded_evidence(error, 420) or 'not recorded'}"
    )
