from copy import deepcopy
from telecapture.recording.events import IDataEvent
from telecapture.transport.serial import unwrap32
from telecapture.transport.receipts import CaptureConflict
from telecapture.transport.forward import plan_forward
from telecapture.transport.transition import CaptureState, PushTransaction

class CaptureReceiver:
    def __init__(self, context, registry, catalog):
        self.context = context
        self.registry = registry
        self.catalog = catalog
        self.state = CaptureState.initial(context)

    def push(self, event):
        self.registry.require_event(self.context, event)
        if isinstance(event, IDataEvent):
            self.catalog.require_stream(event.stream_id, event.evidence.location)
        else:
            self.catalog.validate_forward_entries(event.entries, event.evidence.location)
        transaction = PushTransaction(self)
        state = transaction.stage()
        try:
            if isinstance(event, IDataEvent):
                self._push_data(state, event)
            else:
                self._push_forward(state, event)
            state.queues.refresh(state.messages, state.abandonment)
        except ValueError as exc:
            if isinstance(exc, CaptureConflict):
                raise
            raise CaptureConflict(str(exc), event.evidence.location) from exc
        transaction.commit()

    def _push_data(self, state, event):
        if event.unordered:
            anchor = state.queues.unordered_anchor[event.stream_id]
        else:
            anchor = state.queues.next_mid[event.stream_id]
        logical_mid = unwrap32(event.mid, anchor)
        tsn, duplicate = state.ledger.inspect(event, logical_mid)
        if duplicate:
            return
        if event.unordered:
            initial = self.catalog.require_stream(event.stream_id).initial_unordered_mid
            if (initial <= logical_mid
                    and not state.abandonment.contains(event.stream_id, logical_mid, True)
                    and (event.stream_id, logical_mid) not in state.queues.unordered_delivered):
                state.messages.accept(event, logical_mid)
            state.queues.advance_unordered_anchor(event.stream_id, logical_mid)
        else:
            if (logical_mid >= state.queues.next_mid[event.stream_id] and
                    not state.abandonment.contains(event.stream_id, logical_mid)):
                state.messages.accept(event, logical_mid)
        state.ledger.record(event, tsn, logical_mid)

    def _push_forward(self, state, event):
        plan = plan_forward(event, state.ledger, self.catalog, state.queues)
        state.ledger.advance_cumulative(plan.cumulative)
        state.ledger.forward_position = plan.forward_position
        for stream, unordered, through in plan.abandonments:
            state.abandonment.apply(stream, through, unordered)
            state.messages.retire(stream, through, unordered)
            if unordered:
                state.queues.advance_unordered_anchor(stream, through)

    def snapshot(self):
        return deepcopy(self.state)
