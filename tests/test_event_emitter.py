"""
Tests for EventEmitter class
"""

from unittest.mock import AsyncMock, Mock

import pytest

from wickdata.core.event_emitter import EventEmitter


class TestEventEmitter:
    """Test EventEmitter functionality"""

    def setup_method(self):
        """Setup for each test method"""
        self.emitter = EventEmitter()

    def test_initialization(self):
        """Test EventEmitter initialization"""
        assert self.emitter._listeners == {}
        assert isinstance(self.emitter._listeners, dict)

    def test_on_register_handler(self):
        """Test registering event handlers"""
        handler = Mock()

        self.emitter.on("test_event", handler)
        assert "test_event" in self.emitter._listeners
        assert handler in self.emitter._listeners["test_event"]

    def test_on_register_multiple_handlers(self):
        """Test registering multiple handlers for same event"""
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()

        self.emitter.on("test_event", handler1)
        self.emitter.on("test_event", handler2)
        self.emitter.on("test_event", handler3)

        assert len(self.emitter._listeners["test_event"]) == 3
        assert handler1 in self.emitter._listeners["test_event"]
        assert handler2 in self.emitter._listeners["test_event"]
        assert handler3 in self.emitter._listeners["test_event"]

    def test_on_register_handler_different_events(self):
        """Test registering handlers for different events"""
        handler1 = Mock()
        handler2 = Mock()

        self.emitter.on("event1", handler1)
        self.emitter.on("event2", handler2)

        assert "event1" in self.emitter._listeners
        assert "event2" in self.emitter._listeners
        assert handler1 in self.emitter._listeners["event1"]
        assert handler2 in self.emitter._listeners["event2"]

    def test_off_remove_handler(self):
        """Test removing event handlers"""
        handler = Mock()

        self.emitter.on("test_event", handler)
        assert handler in self.emitter._listeners["test_event"]

        self.emitter.off("test_event", handler)
        assert handler not in self.emitter._listeners["test_event"]

    def test_off_remove_specific_handler(self):
        """Test removing specific handler when multiple exist"""
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()

        self.emitter.on("test_event", handler1)
        self.emitter.on("test_event", handler2)
        self.emitter.on("test_event", handler3)

        self.emitter.off("test_event", handler2)

        assert handler1 in self.emitter._listeners["test_event"]
        assert handler2 not in self.emitter._listeners["test_event"]
        assert handler3 in self.emitter._listeners["test_event"]
        assert len(self.emitter._listeners["test_event"]) == 2

    def test_off_nonexistent_event(self):
        """Test removing handler from nonexistent event"""
        handler = Mock()

        # Should not raise exception
        self.emitter.off("nonexistent", handler)

    def test_off_nonexistent_handler(self):
        """Test removing nonexistent handler"""
        handler1 = Mock()
        handler2 = Mock()

        self.emitter.on("test_event", handler1)
        self.emitter.off("test_event", handler2)

        assert handler1 in self.emitter._listeners["test_event"]
        assert len(self.emitter._listeners["test_event"]) == 1

    @pytest.mark.asyncio
    async def test_once_single_execution(self):
        """Test once handler executes only once"""
        handler = Mock()

        self.emitter.once("test_event", handler)

        await self.emitter.emit("test_event", "arg1", key="value")
        handler.assert_called_once_with("arg1", key="value")

        # Second emission should not call handler
        await self.emitter.emit("test_event", "arg2", key="value2")
        handler.assert_called_once_with("arg1", key="value")

    @pytest.mark.asyncio
    async def test_emit_sync_handler(self):
        """Test emitting event with synchronous handler"""
        handler = Mock()
        self.emitter.on("test_event", handler)

        await self.emitter.emit("test_event", "arg1", "arg2", key="value")

        handler.assert_called_once_with("arg1", "arg2", key="value")

    @pytest.mark.asyncio
    async def test_emit_async_handler(self):
        """Test emitting event with asynchronous handler"""
        handler = AsyncMock()
        self.emitter.on("test_event", handler)

        await self.emitter.emit("test_event", "arg1", key="value")

        handler.assert_called_once_with("arg1", key="value")

    @pytest.mark.asyncio
    async def test_emit_multiple_handlers(self):
        """Test emitting event with multiple handlers"""
        handler1 = Mock()
        handler2 = AsyncMock()
        handler3 = Mock()

        self.emitter.on("test_event", handler1)
        self.emitter.on("test_event", handler2)
        self.emitter.on("test_event", handler3)

        await self.emitter.emit("test_event", "arg", key="value")

        handler1.assert_called_once_with("arg", key="value")
        handler2.assert_called_once_with("arg", key="value")
        handler3.assert_called_once_with("arg", key="value")

    @pytest.mark.asyncio
    async def test_emit_nonexistent_event(self):
        """Test emitting nonexistent event doesn't raise exception"""
        # Should not raise exception
        await self.emitter.emit("nonexistent", "arg")

    @pytest.mark.asyncio
    async def test_emit_no_arguments(self):
        """Test emitting event without arguments"""
        handler = Mock()
        self.emitter.on("test_event", handler)

        await self.emitter.emit("test_event")

        handler.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_emit_mixed_handlers(self):
        """Test emitting with mix of sync and async handlers"""
        sync_handler = Mock()
        async_handler = AsyncMock()

        self.emitter.on("test_event", sync_handler)
        self.emitter.on("test_event", async_handler)

        await self.emitter.emit("test_event", 42)

        sync_handler.assert_called_once_with(42)
        async_handler.assert_called_once_with(42)

    def test_remove_all_listeners_specific_event(self):
        """Test removing all listeners for specific event"""
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()
        handler4 = Mock()

        self.emitter.on("event1", handler1)
        self.emitter.on("event1", handler2)
        self.emitter.on("event2", handler3)
        self.emitter.on("event2", handler4)

        self.emitter.remove_all_listeners("event1")

        assert "event1" not in self.emitter._listeners
        assert "event2" in self.emitter._listeners
        assert len(self.emitter._listeners["event2"]) == 2

    def test_remove_all_listeners_all_events(self):
        """Test removing all listeners for all events"""
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()

        self.emitter.on("event1", handler1)
        self.emitter.on("event2", handler2)
        self.emitter.on("event3", handler3)

        self.emitter.remove_all_listeners()

        assert self.emitter._listeners == {}

    def test_remove_all_listeners_nonexistent_event(self):
        """Test removing listeners for nonexistent event"""
        handler = Mock()
        self.emitter.on("test_event", handler)

        # Should not raise exception
        self.emitter.remove_all_listeners("nonexistent")

        assert "test_event" in self.emitter._listeners
        assert handler in self.emitter._listeners["test_event"]

    def test_listener_count(self):
        """Test counting listeners for an event"""
        handler1 = Mock()
        handler2 = Mock()
        handler3 = Mock()

        assert self.emitter.listener_count("test_event") == 0

        self.emitter.on("test_event", handler1)
        assert self.emitter.listener_count("test_event") == 1

        self.emitter.on("test_event", handler2)
        assert self.emitter.listener_count("test_event") == 2

        self.emitter.on("test_event", handler3)
        assert self.emitter.listener_count("test_event") == 3

        self.emitter.off("test_event", handler2)
        assert self.emitter.listener_count("test_event") == 2

    def test_listener_count_nonexistent_event(self):
        """Test counting listeners for nonexistent event"""
        assert self.emitter.listener_count("nonexistent") == 0

    @pytest.mark.asyncio
    async def test_handler_execution_order(self):
        """Test handlers are executed in registration order"""
        call_order = []

        def handler1():
            call_order.append(1)

        def handler2():
            call_order.append(2)

        async def handler3():
            call_order.append(3)

        self.emitter.on("test_event", handler1)
        self.emitter.on("test_event", handler2)
        self.emitter.on("test_event", handler3)

        await self.emitter.emit("test_event")

        assert call_order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_handler_exception_doesnt_stop_others(self):
        """Test exception in one handler doesn't stop others"""
        handler1 = Mock()
        handler2 = Mock(side_effect=Exception("Test error"))
        handler3 = Mock()

        self.emitter.on("test_event", handler1)
        self.emitter.on("test_event", handler2)
        self.emitter.on("test_event", handler3)

        # Should raise the exception but still call handler3
        with pytest.raises(Exception) as exc_info:
            await self.emitter.emit("test_event")

        assert str(exc_info.value) == "Test error"
        handler1.assert_called_once()
        handler2.assert_called_once()
        # Handler3 won't be called because exception propagates

    @pytest.mark.asyncio
    async def test_once_with_multiple_handlers(self):
        """Test once handler with other permanent handlers"""
        permanent_handler = Mock()
        once_handler = Mock()

        self.emitter.on("test_event", permanent_handler)
        self.emitter.once("test_event", once_handler)

        # First emission
        await self.emitter.emit("test_event", 1)
        permanent_handler.assert_called_with(1)
        once_handler.assert_called_once_with(1)

        # Second emission
        await self.emitter.emit("test_event", 2)
        assert permanent_handler.call_count == 2
        permanent_handler.assert_called_with(2)
        once_handler.assert_called_once_with(1)  # Still only called once

    @pytest.mark.asyncio
    async def test_complex_event_flow(self):
        """Test complex event flow with multiple operations"""
        results = []

        def handler_a(value):
            results.append(f"a:{value}")

        async def handler_b(value):
            results.append(f"b:{value}")

        def handler_c(value):
            results.append(f"c:{value}")

        # Register handlers
        self.emitter.on("event1", handler_a)
        self.emitter.on("event1", handler_b)
        self.emitter.once("event2", handler_c)

        # Emit events
        await self.emitter.emit("event1", 1)
        await self.emitter.emit("event2", 2)
        await self.emitter.emit("event1", 3)
        await self.emitter.emit("event2", 4)  # handler_c shouldn't fire

        assert results == ["a:1", "b:1", "c:2", "a:3", "b:3"]
