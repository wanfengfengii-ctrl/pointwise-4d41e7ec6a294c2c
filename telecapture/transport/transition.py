from copy import deepcopy
from dataclasses import dataclass
from telecapture.transport.receipts import ReceiptLedger
from telecapture.delivery.messages import MessageStore
from telecapture.delivery.abandonment import AbandonmentIndex
from telecapture.delivery.queues import DeliveryQueues

@dataclass
class CaptureState:
    ledger: ReceiptLedger
    messages: MessageStore
    abandonment: AbandonmentIndex
    queues: DeliveryQueues

    @classmethod
    def initial(cls, context):
        starts = {(stream, False): mid for stream, mid in context.streams}
        starts.update({(stream, True): mid
                       for stream, mid in context.unordered_initial_map.items()})
        return cls(ReceiptLedger.initial(context.initial_cumulative_tsn), MessageStore(),
                   AbandonmentIndex(starts), DeliveryQueues.initial(context))

class PushTransaction:
    def __init__(self, receiver):
        self.receiver = receiver
        self.original = receiver.state
        self.work = None

    def stage(self):
        if self.work is not None:
            raise RuntimeError('transaction already staged')
        self.work = deepcopy(self.original)
        return self.work

    def commit(self):
        if self.work is None or self.receiver.state is not self.original:
            raise RuntimeError('invalid push transaction')
        self.receiver.state = self.work
        self.work = None
