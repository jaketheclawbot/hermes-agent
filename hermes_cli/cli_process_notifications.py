"""CLI notification ownership, structured queueing and durable turn acceptance."""


class _DurableCompletionMessage:
    """Synthetic turn whose producer is acknowledged after the turn returns."""

    __slots__ = ("text", "event", "claim", "deliveries")

    def __init__(self, text, event: dict, claim: str, deliveries=None):
        self.text = text
        self.event = event
        self.claim = claim
        self.deliveries = tuple(deliveries or ((event, claim),))


class CLIProcessNotificationsMixin:
    def _owns_process_notification(self, event: dict) -> bool:
        """Whether this session owns a delegation event (pre-compression keys resolve to their continuation; fail closed)."""
        event_key = str(event.get("session_key") or "")
        current_key = str(getattr(self, "session_id", "") or "")
        if not event_key or not current_key:
            return False
        if event_key == current_key:
            return True
        try:
            session_db = getattr(self, "_session_db", None)
            resolved_key = (
                session_db.resolve_resume_session_id(event_key) if session_db is not None else event_key
            ) or event_key
        except Exception:
            resolved_key = event_key
        return str(resolved_key) == current_key

    def _background_notifications_suppressed(self) -> bool:
        """Whether ``display.background_process_notifications`` is ``off`` for this CLI session.

        The key gates the gateway's completion injection (#9290) but the CLI drain never consulted
        it, so the documented ``off`` escape hatch silently did nothing here (#123114). Mirrors the
        gateway semantics: events are still drained, claimed and acknowledged — only the
        turn-starting injection is suppressed."""
        try:
            from cli import CLI_CONFIG
            mode = str((CLI_CONFIG.get("display") or {}).get("background_process_notifications") or "").strip().lower()
        except Exception:
            return False
        return mode == "off"

    def _drain_process_notifications(self, consumer: str) -> None:
        from tools.process_registry import process_registry
        from tools.async_delegation import claim_event_delivery, complete_event_delivery
        from tools.process_registry_notifications import (
            HEARTBEAT_DISPLAY_KIND, ProcessNotificationBatch, TimelineNotification, group_process_notifications,
            heartbeat_display_text)

        claimed = []
        for event, text in process_registry.drain_notifications(
            session_key=getattr(self, "session_id", "") or "", owns_event=self._owns_process_notification,
        ):
            claim = claim_event_delivery(event, consumer)
            if claim is None:
                continue
            claimed.append((event, text, claim))
        if self._background_notifications_suppressed():
            # Subagent results are not process notifications: they still land.
            visible = []
            for event, text, claim in claimed:
                if event.get("type") == "async_delegation":
                    visible.append((event, text, claim))
                else:
                    complete_event_delivery(event, claim)
            claimed = visible
        pairs = [(event, text) for event, text, _claim in claimed]
        claims = {id(event): claim for event, _text, claim in claimed}
        for notifications in group_process_notifications(pairs):
            event, text = notifications[0]
            evt_type = event.get("type", "completion")
            if evt_type == "completion":
                pending = ProcessNotificationBatch(notifications)
            elif evt_type == "heartbeat":
                pending = TimelineNotification(text, heartbeat_display_text(event), HEARTBEAT_DISPLAY_KIND)
            else:
                pending = TimelineNotification.for_delegation(text, event) if evt_type == "async_delegation" else text
                from agent.notification_presentation import diagnostic_process_event
                if diagnostic_process_event(event) and not isinstance(pending, TimelineNotification):
                    pending = TimelineNotification(text, text, "internal_notification", "diagnostic")
            deliveries = tuple((item, claims[id(item)]) for item, _text in notifications)
            self._pending_input.put(
                _DurableCompletionMessage(pending, event, deliveries[0][1], deliveries)
            )

    def _acknowledge_durable_completion(self, message: _DurableCompletionMessage) -> bool:
        """Acknowledge every producer only after its synthetic turn is durable."""
        from tools.async_delegation import complete_event_delivery
        from tools.process_registry import process_registry

        accepted = True
        for event, claim in message.deliveries:
            complete_event_delivery(event, claim)
            if event.get("type") == "completion":
                session_id = str(event.get("session_id") or "")
                if not process_registry.acknowledge_terminal_notification(session_id):
                    pending = getattr(self, "_pending_terminal_ack_retries", None)
                    if not isinstance(pending, set):
                        pending = set()
                        self._pending_terminal_ack_retries = pending
                    pending.add(session_id)
                    accepted = False
        return accepted

    def _retry_terminal_acknowledgements(self) -> None:
        from tools.process_registry import process_registry

        process_registry.retry_accepted_terminal_acknowledgements()
        pending = getattr(self, "_pending_terminal_ack_retries", None)
        if not isinstance(pending, set):
            return
        for session_id in tuple(pending):
            if process_registry.acknowledge_terminal_notification(session_id):
                pending.discard(session_id)

    def _finish_terminal_turn_acceptance(
        self, durable_completion, owner_session_id: str, observed_ids=None,
    ) -> bool:
        """Commit only producer obligations accepted by this exact durable turn."""
        from tools.async_delegation import release_event_delivery
        from tools.process_registry import process_registry

        observed = {} if observed_ids is None else {"observed_ids": observed_ids}
        if getattr(self, "_last_turn_durably_accepted", False) is not True:
            if durable_completion is not None:
                for event, claim in durable_completion.deliveries:
                    release_event_delivery(event, claim)
                    if (event.get("type") != "completion"
                            or event.get("session_id") not in (observed_ids or ())):
                        process_registry.completion_queue.put(event)
            process_registry.release_consumed_terminal_notifications(
                owner_session_id, **observed
            )
            return False
        if durable_completion is not None:
            self._acknowledge_durable_completion(durable_completion)
        return process_registry.acknowledge_consumed_terminal_notifications(
            owner_session_id, **observed
        )

    def _chat_with_terminal_acceptance(self, message, durable_completion=None, **kwargs):
        from tools.process_registry import begin_terminal_observation_turn, end_terminal_observation_turn

        owner = self.session_id or ""
        if durable_completion is not None and not all(
            self._owns_process_notification(event)
            for event, _claim in durable_completion.deliveries
        ):
            self._last_turn_durably_accepted = False
            self._finish_terminal_turn_acceptance(durable_completion, owner, set())
            return None
        turn, token = begin_terminal_observation_turn(owner)
        self._last_turn_durably_accepted = False
        try:
            return self.chat(message, **kwargs)
        except BaseException:
            self._last_turn_durably_accepted = False
            raise
        finally:
            end_terminal_observation_turn(turn, token)
            self._finish_terminal_turn_acceptance(
                durable_completion, owner, turn["observed"]
            )

    def _record_chat_turn_acceptance(self, result) -> bool:
        """Publish a receipt only for a successful, durably flushed turn."""
        accepted = bool(
            isinstance(result, dict)
            and result.get("completed") is True
            and result.get("failed") is not True
            and result.get("partial") is not True
            and result.get("interrupted") is not True
            and not result.get("cleanup_errors")
            and not getattr(self, "_last_turn_interrupted", False)
        )
        if accepted:
            try:
                accepted = self.agent._flush_messages_to_session_db(
                    self.conversation_history, None
                ) is True
            except Exception:
                accepted = False
        self._last_turn_durably_accepted = accepted
        return accepted

    def _tui_unwrap_input(self, user_input):
        """Unwrap ``_VoiceInputMessage`` / ``_SeededQueryMessage`` -> ``(text_or_tuple, is_voice_input, is_seeded_query)``."""
        from cli import _VoiceInputMessage, _SeededQueryMessage
        from tools.process_registry import process_registry
        from tools.process_registry_notifications import (
            PROCESS_COMPLETE_DISPLAY_KIND, ProcessNotificationBatch, TimelineNotification)
        if isinstance(user_input, _DurableCompletionMessage):
            user_input = user_input.text
        if isinstance(user_input, ProcessNotificationBatch):
            rendered = user_input.render(process_registry)
            user_input = rendered and TimelineNotification(
                rendered, user_input.display_text(process_registry), PROCESS_COMPLETE_DISPLAY_KIND)
        # Voice-transcribed messages arrive wrapped in a sentinel so only genuine STT output gets the voice
        # prefix (#65827).
        is_voice_input = isinstance(user_input, _VoiceInputMessage)
        if is_voice_input:
            user_input = user_input.text
        is_seeded_query = isinstance(user_input, _SeededQueryMessage)
        if is_seeded_query:
            user_input = (user_input.text, user_input.images) if user_input.images else user_input.text
        return user_input, is_voice_input, is_seeded_query
