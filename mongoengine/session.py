from __future__ import annotations

from contextvars import ContextVar
from typing import Any, Optional

# Immutable tuple stack for copy-on-write behavior
_SESSIONS_STACK: ContextVar[tuple[Any, ...]] = ContextVar(
    "mongoengine_sessions_stack",
    default=(),
)


def _set_session(session: Any):
    """
    Push a session onto the task/thread-local stack.
    Returns a ContextVar Token, which you SHOULD store and reset on exit.
    """
    stack = _SESSIONS_STACK.get()
    return _SESSIONS_STACK.set(stack + (session,))


def _get_session() -> Any | None:
    """Return the current (top) session, or None."""
    stack = _SESSIONS_STACK.get()
    return stack[-1] if stack else None


def _clear_session(token=None):
    """
    Pop the current session.

    Best practice: pass the token returned by _set_session(session),
    so the stack is restored exactly even if something else touched it.
    """
    if token is not None:
        _SESSIONS_STACK.reset(token)
        return

    # Fallback pop (less strict): remove the top if present.
    stack = _SESSIONS_STACK.get()
    if stack:
        _SESSIONS_STACK.set(stack[:-1])


def _clear_all_sessions():
    """Clear the stack entirely for the current context."""
    _SESSIONS_STACK.set(())
