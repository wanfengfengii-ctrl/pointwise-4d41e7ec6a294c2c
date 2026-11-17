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
        key = (event.stream_id, logical_mid)
        message = self.active.get(key)
        if message is None:
            message = MessageState(event.stream_id, logical_mid, event.mid)
            self.active[key] = message
        message.fragments.add(Fragment.from_event(event))

    def complete_message(self, stream, logical_mid):
        state = self.active.get((stream, logical_mid))
        if state is None or not state.fragments.is_complete():
            return None
        body, fragments = state.fragments.assemble()
        return ReassembledMessage(stream, logical_mid, state.raw_mid, body, fragments)

    def retire(self, stream, through):
        for key in tuple(self.active):
            if key[0] == stream and key[1] <= through:
                del self.active[key]

    def incomplete(self):
        return tuple(sorted(key for key, item in self.active.items()
                            if not item.fragments.is_complete()))
