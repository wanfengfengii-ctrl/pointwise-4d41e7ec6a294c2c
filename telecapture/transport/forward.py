from dataclasses import dataclass
from telecapture.transport.serial import unwrap32

@dataclass(frozen=True)
class ForwardPlan:
    cumulative: int
    forward_position: int
    abandonments: tuple

def plan_forward(event, ledger, catalog, queues):
    catalog.validate_forward_entries(event.entries, event.evidence.location)
    position = unwrap32(event.cumulative_tsn, ledger.forward_position)
    if position < ledger.forward_position:
        return ForwardPlan(ledger.cumulative, ledger.forward_position, ())
    limits = {}
    for entry in event.entries:
        if entry.unordered:
            anchor = queues.unordered_anchor[entry.stream_id]
        else:
            anchor = queues.next_mid[entry.stream_id]
        logical = unwrap32(entry.mid, anchor)
        key = (entry.stream_id, entry.unordered)
        limits[key] = max(limits.get(key, logical), logical)
    abandonments = tuple(sorted((stream, unordered, logical)
                                for (stream, unordered), logical in limits.items()))
    return ForwardPlan(max(ledger.cumulative, position), position, abandonments)
