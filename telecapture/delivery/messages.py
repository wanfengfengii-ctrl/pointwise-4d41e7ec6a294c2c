from dataclasses import dataclass, field
from telecapture.delivery.fragments import Fragment, FragmentSet

@dataclass
class MessageState:
    stream_id: int
    logical_mid: int
    raw_mid: int
    fragments: FragmentSet = field(default_factory=FragmentSet)

@dataclass(frozen=True)
class ReassembledMessage:
    stream_id: int
    logical_mid: int
    raw_mid: int
    body: bytes
    fragments: tuple

@dataclass
class MessageStore:
    active: dict = field(default_factory=dict)

    def accept(self, event, logical_mid):
        key = (event.stream_id, event.unordered, logical_mid)
        message = self.active.get(key)
        if message is None:
            message = MessageState(event.stream_id, logical_mid, event.mid)
            self.active[key] = message
        message.fragments.add(Fragment.from_event(event))

    @staticmethod
    def _assemble(state):
        body, fragments = state.fragments.assemble()
        return ReassembledMessage(state.stream_id, state.logical_mid, state.raw_mid,
                                  body, fragments)

    def complete_message(self, stream, logical_mid, unordered=False):
        state = self.active.get((stream, unordered, logical_mid))
        if state is None or not state.fragments.is_complete():
            return None
        return self._assemble(state)

    def complete_messages(self, stream, unordered=False):
        found = [self._assemble(state) for (s, u, _), state in self.active.items()
                 if s == stream and u == unordered and state.fragments.is_complete()]
        return tuple(sorted(found, key=lambda item: item.logical_mid))

    def retire(self, stream, through, unordered=False):
        for key in tuple(self.active):
            if key[0] == stream and key[1] == unordered and key[2] <= through:
                del self.active[key]

    def remove(self, stream, unordered, logical_mid):
        self.active.pop((stream, unordered, logical_mid), None)

    def incomplete(self):
        pending = sorted((key[0], key[1], key[2]) for key, item in self.active.items()
                         if not item.fragments.is_complete())
        return tuple((stream, logical) if not unordered else (stream, logical, True)
                     for stream, unordered, logical in pending)
