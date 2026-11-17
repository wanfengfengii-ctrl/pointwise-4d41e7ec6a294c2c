from dataclasses import dataclass, field
from telecapture.transport.receipts import CaptureConflict

@dataclass(frozen=True)
class Fragment:
    fsn: int
    begin: bool
    end: bool
    payload: bytes
    tsn: int
    evidence: object

    @classmethod
    def from_event(cls, event):
        return cls(event.fsn, event.begin, event.end, event.payload, event.tsn, event.evidence)

@dataclass
class FragmentSet:
    pieces: dict = field(default_factory=dict)
    last: object = None

    def add(self, fragment):
        loc = fragment.evidence.location
        if fragment.begin != (fragment.fsn == 0):
            raise CaptureConflict('only FSN0 must carry begin', loc)
        old = self.pieces.get(fragment.fsn)
        if old is not None:
            if (old.begin, old.end, old.payload) != (fragment.begin, fragment.end, fragment.payload):
                raise CaptureConflict('FSN content or marker conflict', loc)
            return
        if self.last is not None and (fragment.fsn > self.last or
                                      (fragment.end and fragment.fsn != self.last)):
            raise CaptureConflict('fragment beyond established end', loc)
        if fragment.end and any(fsn > fragment.fsn for fsn in self.pieces):
            raise CaptureConflict('end precedes received fragment', loc)
        self.pieces[fragment.fsn] = fragment
        if fragment.end:
            self.last = fragment.fsn

    def is_complete(self):
        return (self.last is not None and 0 in self.pieces and self.pieces[0].begin
                and len(self.pieces) == self.last + 1)

    def assemble(self):
        if not self.is_complete():
            raise CaptureConflict('cannot assemble an incomplete message')
        ordered = tuple(self.pieces[i] for i in range(self.last + 1))
        return b''.join(part.payload for part in ordered), ordered
