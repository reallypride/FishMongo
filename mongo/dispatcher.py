class Signal(object):
    """
    Base class for all signals
    
    Internal attributes:
    
        receivers
            { receriverkey (id) : weakref(receiver) }
    """
    def __init__(self, providing_args=None):
        """
        Create a new signal.
        
        providing_args
            A list of the arguments this signal can pass along in a send() call.
        """
        self.receivers = []
        if providing_args is None:
            providing_args = []
        self.providing_args = set(providing_args)

    def connect(self, receiver, sender=None):
        lookup_key = id(sender)
        self.receivers.append((lookup_key, receiver))

    def disconnect(self, receiver=None, sender=None):
        lookup_key = id(sender)
        for index in xrange(len(self.receivers)):
            (r_key, _) = self.receivers[index]
            if r_key == lookup_key:
                del self.receivers[index]
                break

    def send(self, sender, **named):
        """
        Send signal from sender to all connected receivers.

        If any receiver raises an error, the error propagates back through send,
        terminating the dispatch loop, so it is quite possible to not have all
        receivers called if a raises an error.

        Arguments:
        
            sender
                The sender of the signal Either a specific object or None.
    
            named
                Named arguments which will be passed to receivers.

        Returns a list of tuple pairs [(receiver, response), ... ].
        """
        responses = []
        if not self.receivers:
            return responses

        for receiver in self._live_receivers(sender):
            response = receiver(signal=self, sender=sender, **named)
            responses.append((receiver, response))
        return responses

    def _live_receivers(self, sender):
        """
        Filter sequence of receivers to get resolved, live receivers.

        This checks for weak references and resolves them, then returning only
        live receivers.
        """
        senderkey = id(sender)
        receivers = []

        for key, receiver in self.receivers:
            if key == senderkey:
                receivers.append(receiver)
        return receivers

def receiver(signal, **kwargs):
    """
    A decorator for connecting receivers to signals. Used by passing in the
    signal and keyword arguments to connect::

        @receiver(post_save, sender=MyModel)
        def signal_receiver(sender, **kwargs):
            ...

    """
    def _decorator(func):
        signal.connect(func, **kwargs)
        return func
    return _decorator
