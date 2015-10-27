import sys

from celery import task
from django.dispatch.dispatcher import (
    NO_RECEIVERS,
    Signal,
)


class AsyncSignal(Signal):

    def __init__(self, providing_args=None, queue=None):

        super(AsyncSignal, self).__init__(providing_args=providing_args)
        self.queue = queue

    def send(self, sender, **named):
        """Send the signal via Celery."""
        return self._send.apply_async(
            args=(sender,),
            kwargs=named,
            queue=self.queue,
        )

    def send_robust(self, sender, **named):
        return self._send_robust.apply_async(
            args=(sender,),
            kwargs=named,
            queue=self.queue,
        )

    @task
    def _send(self, sender, **named):
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
        if not self.receivers or self.sender_receivers_cache.get(sender) is NO_RECEIVERS:
            return responses

        for receiver in self._live_receivers(sender):
            response = receiver(signal=self, sender=sender, **named)
            responses.append((receiver, response))
        return responses

    @task
    def _send_robust(self, sender, **named):
        """
        Send signal from sender to all connected receivers catching errors.
        Arguments:
            sender
                The sender of the signal. Can be any python object (normally one
                registered with a connect if you actually want something to
                occur).
            named
                Named arguments which will be passed to receivers. These
                arguments must be a subset of the argument names defined in
                providing_args.
        Return a list of tuple pairs [(receiver, response), ... ]. May raise
        DispatcherKeyError.
        If any receiver raises an error (specifically any subclass of
        Exception), the error instance is returned as the result for that
        receiver. The traceback is always attached to the error at
        ``__traceback__``.
        """
        responses = []
        if not self.receivers or self.sender_receivers_cache.get(sender) is NO_RECEIVERS:
            return responses

        # Call each receiver with whatever arguments it can accept.
        # Return a list of tuple pairs [(receiver, response), ... ].
        for receiver in self._live_receivers(sender):
            try:
                response = receiver(signal=self, sender=sender, **named)
            except Exception as err:
                if not hasattr(err, '__traceback__'):
                    err.__traceback__ = sys.exc_info()[2]
                responses.append((receiver, err))
            else:
                responses.append((receiver, response))
        return responses
