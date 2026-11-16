"""Release/expiry desktop notifications on the existing gateway watcher rail."""
import asyncio
import contextlib
import logging
import os

logger = logging.getLogger(__name__)


async def watch_desktop_waits(self, interval: float = 1.0) -> None:
    """Wake parked desktop turns or report their three-hour expiry."""
    from tools.computer_use.desktop_lease import (
        claim_desktop_wait_events,
        complete_desktop_wait_event,
        discard_desktop_wait,
    )

    consumer_id = f"gateway:{os.getpid()}:{id(self)}"
    while self._running:
        events = await asyncio.to_thread(claim_desktop_wait_events, consumer_id)
        for evt in events:
            if evt.get("cron_session_id"):
                # Cron sessions are read-only to generic watch notices. Admit
                # a private same-fire continuation, never another cron fire.
                from gateway.session_desktop_wait import deliver_cron_wait_for_runner
                try:
                    delivered = await deliver_cron_wait_for_runner(self, evt)
                except Exception:
                    logger.exception("Cron desktop continuation admission failed")
                    delivered = False
                await asyncio.to_thread(
                    complete_desktop_wait_event, str(evt.get("wait_id") or ""),
                    str(evt.get("claim_id") or ""), delivered=delivered,
                )
                continue
            delivered = False
            parent_session_id = str(evt.get("parent_session_id") or "").strip()
            if parent_session_id:
                verdict = await self._classify_completion_target(parent_session_id)
                if verdict == "terminal":
                    await asyncio.to_thread(
                        discard_desktop_wait, str(evt.get("wait_id") or "")
                    )
                    continue
                if verdict == "retry":
                    await asyncio.to_thread(
                        complete_desktop_wait_event,
                        str(evt.get("wait_id") or ""),
                        str(evt.get("claim_id") or ""),
                        delivered=False,
                    )
                    continue
            try:
                if evt.get("type") == "desktop_wait_ready":
                    synth_text = (
                        "[IMPORTANT: The shared desktop is now available for your "
                        "parked Computer Use request. Re-read the latest user messages "
                        "before acting. If the request was superseded or cancelled, do "
                        "not execute stale UI work. Otherwise resume the original task "
                        "now by retrying the previously blocked desktop action. Do not "
                        "send a mere status update; continue through verified completion.]"
                    )
                    delivered = bool(
                        await self._inject_watch_notification(synth_text, evt)
                    )
                else:
                    source = await asyncio.to_thread(
                        self._build_process_event_source, evt
                    )
                    adapter = self._adapter_for_source(source) if source else None
                    if source and adapter:
                        result = await adapter.send(
                            source.chat_id,
                            "⚠️ The queued Computer Use request expired after waiting "
                            "three hours for the shared desktop. No desktop action was "
                            "executed. If the task is still wanted, send a new message "
                            "in this thread to queue it again.",
                            metadata=self._thread_metadata_for_source(source),
                        )
                        delivered = bool(getattr(result, "success", False))
            except Exception:
                logger.exception(
                    "Desktop wait event delivery failed: wait_id=%s type=%s",
                    evt.get("wait_id"), evt.get("type"),
                )
            await asyncio.to_thread(
                complete_desktop_wait_event,
                str(evt.get("wait_id") or ""),
                str(evt.get("claim_id") or ""),
                delivered=delivered,
            )
        signal = getattr(self, "_desktop_wait_signal", None)
        if signal is None:
            await asyncio.sleep(interval)
            continue
        try:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(signal.wait(), timeout=interval)
        finally:
            signal.clear()
