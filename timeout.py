"""Shared timeout decorator using SIGALRM.

Provides a reusable ``@with_timeout()`` decorator and a single
``ToolTimeoutError`` exception used across MCP servers and the
Redshift connection factory.
"""

import signal
from functools import wraps


class ToolTimeoutError(Exception):
    """Raised when an operation exceeds its time budget."""


def with_timeout(seconds: int):
    """Decorator that aborts the wrapped function after *seconds* via SIGALRM.

    Restores the previous signal handler on exit (success or failure).
    Unix-only — ``signal.SIGALRM`` is not available on Windows.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            def _handler(signum, frame):
                raise ToolTimeoutError(f"Operation timed out after {seconds} seconds")

            old_handler = signal.signal(signal.SIGALRM, _handler)
            signal.alarm(seconds)
            try:
                return func(*args, **kwargs)
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)

        return wrapper

    return decorator
