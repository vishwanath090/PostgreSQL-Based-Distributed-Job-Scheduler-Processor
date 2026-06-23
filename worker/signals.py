"""
worker/signals.py
-----------------
Shared process-wide asyncio shutdown signal.

Lives in its own tiny module so worker.py and heartbeat.py can both import
it without creating a circular dependency.

Usage
-----
    from worker.signals import shutdown_event

    # In the signal handler:
    shutdown_event.set()

    # In any loop:
    while not shutdown_event.is_set():
        ...
"""

import asyncio

# Set once (by SIGTERM / SIGINT handler or tests) to tell every coroutine
# in this process to finish its current unit of work and exit cleanly.
shutdown_event: asyncio.Event = asyncio.Event()
