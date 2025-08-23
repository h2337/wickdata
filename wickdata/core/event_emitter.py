"""
Event emitter base class for streaming components
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional


class EventEmitter:
    """Base class for event-driven components"""

    def __init__(self) -> None:
        """Initialize event emitter"""
        self._listeners: Dict[str, List[Callable]] = {}

    def on(self, event: str, handler: Callable) -> None:
        """
        Register an event handler

        Args:
            event: Event name
            handler: Event handler function
        """
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(handler)

    def off(self, event: str, handler: Callable) -> None:
        """
        Remove an event handler

        Args:
            event: Event name
            handler: Event handler function
        """
        if event in self._listeners:
            self._listeners[event] = [h for h in self._listeners[event] if h != handler]

    def once(self, event: str, handler: Callable) -> None:
        """
        Register a one-time event handler

        Args:
            event: Event name
            handler: Event handler function
        """

        def wrapper(*args: Any, **kwargs: Any) -> None:
            handler(*args, **kwargs)
            self.off(event, wrapper)

        self.on(event, wrapper)

    async def emit(self, event: str, *args: Any, **kwargs: Any) -> None:
        """
        Emit an event

        Args:
            event: Event name
            *args: Event arguments
            **kwargs: Event keyword arguments
        """
        if event in self._listeners:
            for handler in self._listeners[event]:
                if asyncio.iscoroutinefunction(handler):
                    await handler(*args, **kwargs)
                else:
                    handler(*args, **kwargs)

    def remove_all_listeners(self, event: Optional[str] = None) -> None:
        """
        Remove all listeners for an event or all events

        Args:
            event: Event name (optional)
        """
        if event:
            self._listeners.pop(event, None)
        else:
            self._listeners.clear()

    def listener_count(self, event: str) -> int:
        """
        Get number of listeners for an event

        Args:
            event: Event name

        Returns:
            Number of listeners
        """
        return len(self._listeners.get(event, []))
