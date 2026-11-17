from dataclasses import dataclass, field
from telecapture.transport.serial import unwrap32

class CaptureConflict(ValueError):
    def __init__(self, message, location=None):
        self.location = location
        super().__init__(f'{location}: {message}' if location else message)

@dataclass(frozen=True)
class Receipt:
    logical_tsn: int
    stream_id: int
    unordered: bool
    logical_mid: int
    fsn: int
    signature: tuple
    evidence: object

@dataclass
class ReceiptLedger:
    cumulative: int
    forward_position: int
    receipts: dict = field(default_factory=dict)
    fragments: dict = field(default_factory=dict)

    @classmethod
    def initial(cls, cumulative):
        return cls(cumulative, cumulative)

    @staticmethod
    def signature(event, logical_mid):
        return (event.session_id, event.stream_id, event.unordered, logical_mid,
                event.fsn, event.begin, event.end, event.payload)

    def inspect(self, event, logical_mid):
        tsn = unwrap32(event.tsn, self.cumulative)
        signature = self.signature(event, logical_mid)
        previous = self.receipts.get(tsn)
        if previous is not None:
            if previous.signature != signature:
                raise CaptureConflict('TSN identity conflict', event.evidence.location)
            return tsn, True
        fragment_key = (event.stream_id, event.unordered, logical_mid, event.fsn)
        fragment_value = (event.begin, event.end, event.payload)
        if fragment_key in self.fragments and self.fragments[fragment_key] != fragment_value:
            raise CaptureConflict('fragment identity conflict', event.evidence.location)
        return tsn, False

    def record(self, event, logical_tsn, logical_mid):
        self.receipts[logical_tsn] = Receipt(logical_tsn, event.stream_id, event.unordered,
                                             logical_mid, event.fsn,
                                             self.signature(event, logical_mid), event.evidence)
        self.fragments[(event.stream_id, event.unordered, logical_mid, event.fsn)] = (
            event.begin, event.end, event.payload)
        self.advance_cumulative(self.cumulative)

    def advance_cumulative(self, position):
        self.cumulative = max(self.cumulative, position)
        while self.cumulative + 1 in self.receipts:
            self.cumulative += 1
