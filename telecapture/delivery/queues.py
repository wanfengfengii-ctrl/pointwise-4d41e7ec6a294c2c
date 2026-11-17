import json
from dataclasses import dataclass, field

def make_delivery_key(recording_id, gateway_id, session_id, stream_id, logical_mid):
    return json.dumps([recording_id, gateway_id, session_id, stream_id, logical_mid],
                      ensure_ascii=True, separators=(',', ':'))

@dataclass(frozen=True)
class DeliveredMessage:
    recording_id: str
    gateway_id: str
    session_id: str
    stream_id: int
    logical_mid: int
    raw_mid: int
    body: bytes
    fragments: tuple = ()

    @property
    def key(self):
        return make_delivery_key(self.recording_id, self.gateway_id, self.session_id,
                                 self.stream_id, self.logical_mid)

@dataclass
class DeliveryQueues:
    context: object
    next_mid: dict
    ready: dict = field(default_factory=dict)

    @classmethod
    def initial(cls, context):
        return cls(context, dict(context.streams))

    def refresh(self, messages, abandonment):
        for stream in sorted(self.next_mid):
            position = abandonment.next_unabandoned(stream, self.next_mid[stream])
            self.next_mid[stream] = position
            pending = []
            while True:
                position = abandonment.next_unabandoned(stream, position)
                candidate = messages.complete_message(stream, position)
                if candidate is None:
                    break
                pending.append(candidate)
                position += 1
            self.ready[stream] = tuple(pending)

    def drain(self, messages, abandonment):
        self.refresh(messages, abandonment)
        result = []
        for stream in sorted(self.next_mid):
            for item in self.ready.get(stream, ()):
                result.append(DeliveredMessage(
                    self.context.recording_id, self.context.gateway_id, self.context.session_id,
                    stream, item.logical_mid, item.raw_mid, item.body, item.fragments))
                self.next_mid[stream] = item.logical_mid + 1
            self.next_mid[stream] = abandonment.next_unabandoned(stream, self.next_mid[stream])
            messages.retire(stream, self.next_mid[stream] - 1)
        self.ready = {stream: () for stream in self.next_mid}
        return result
