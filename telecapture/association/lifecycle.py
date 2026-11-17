from telecapture.association.context import AssociationError
from telecapture.association.registry import RecordingRegistry
from telecapture.association.streams import StreamCatalog
from telecapture.transport.receiver import CaptureReceiver

class CaptureSession:
    def __init__(self, header, registry=None):
        self.registry = registry if registry is not None else RecordingRegistry()
        self.context = self.registry.register(header)
        self.catalog = StreamCatalog(self.context)
        self.receiver = CaptureReceiver(self.context, self.registry, self.catalog)
        self.input_finished = False

    def push(self, event):
        if self.input_finished:
            raise AssociationError('recording input is already finished', event.evidence.location)
        self.receiver.push(event)

    def drain(self):
        state = self.receiver.state
        return state.queues.drain(state.messages, state.abandonment)

    def finish_input(self):
        self.input_finished = True
        return self.receiver.state.messages.incomplete()

def open_session(header, registry=None):
    return CaptureSession(header, registry)
