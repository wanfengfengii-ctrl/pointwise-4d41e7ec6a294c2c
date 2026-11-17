from telecapture.association.context import make_context, AssociationError
from telecapture.recording.events import IDataEvent, ForwardEvent

def assert_same_registration(old, new, location=None):
    if old != new:
        raise AssociationError('recording identity has conflicting header', location)

class RecordingRegistry:
    def __init__(self):
        self.contexts = {}

    def register(self, header):
        context = make_context(header)
        previous = self.contexts.get(context.registration_key)
        if previous is not None:
            assert_same_registration(previous.canonical, context.canonical)
            return previous
        self.contexts[context.registration_key] = context
        return context

    def require_event(self, context, event):
        if not isinstance(event, (IDataEvent, ForwardEvent)):
            raise AssociationError('expected a decoded recording event')
        if event.session_id != context.session_id:
            raise AssociationError('event belongs to another session', event.evidence.location)
