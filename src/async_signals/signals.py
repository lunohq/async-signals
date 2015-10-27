from async_signals.dispatcher import AsyncSignal

# Request lifecycle signals
request_started = AsyncSignal(providing_args=["environ"])
request_finished = AsyncSignal()
