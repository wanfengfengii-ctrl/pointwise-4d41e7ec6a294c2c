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
        logical = unwrap32(entry.mid, queues.next_mid[entry.stream_id])
        limits[entry.stream_id] = max(limits.get(entry.stream_id, logical), logical)
    return ForwardPlan(max(ledger.cumulative, position), position, tuple(sorted(limits.items())))
