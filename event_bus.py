import abc
import json
import unittest


class EventSource:
    """Abstract type for classes that publish events.
    """

    @abc.abstractmethod
    def name(self):
        """Returns the name of this event source, for grouping.

        For example, this method might return "allocator" or "scheduler".
        """

        raise NotImplementedError

    @abc.abstractmethod
    def id(self):
        """Returns the id of this event source, for disambuation.

        For example, this method might return "drf_allocator" or
        "first_fit_scheduler".
        """

        raise NotImplementedError


class EventHandler(object):
    """Abstract type of event output delegates.
    """

    @abc.abstractmethod
    def handle(event):
        """Sink the supplied event.

        :param event: Event to handle.
        :type event: dict
        """
        raise NotImplementedError


class JsonFileEventWriter(EventHandler):
    """Event handler that emits event data as JSON to a file.
    """

    def __init__(self, out_file_path):
        """
        :param out_file_path: Path to the event output file.
        :type out_file_path: str
        """

        self.out_file = open(out_file_path, "w+")

    def handle(self, event):
        json.dump(event, self.out_file)
        self.out_file.write("\n")


class EventBus:
    """
    """

    def __init__(self, handlers, current_time):
        """
        :param handlers: Collection of event output delegates.
        :type handlers: list of EventHandler

        :param current_time: A nullary function that returns the current
                             simulation time-step (an integer).
        :type current_time: function () => integer
        """
        self.handlers = handlers
        self.current_time = current_time

    def publish(self, source, event_name, data):
        """Forwards an event to all of the configured event handlers.

        :param source: The source of the event.
        :type source: EventSource

        :param event_name: The name of the event, for grouping. For example,
                           this value might be "offer_issued" or
                           "task_finished".
        :type event_name: str

        :param data: Arbitrary additional data to describe the event. For
                     example, this could describe resources that were offered
                     to a scheduler, or the sizes and durations of the tasks
                     in a scheduler queue.
        :type data: mixed (dict, str, number, list)
        """

        event = {}
        event["name"] = event_name
        event["data"] = data
        event["source"] = source.name()
        event["id"] = source.id()
        event["time"] = self.current_time()

        for h in self.handlers:
            h.handle(event)


def _get_event_handlers(config):
    """Returns a list of the configured event handler instances.

    :param config: A global program configuration object
    :type config: dict

    :rtype: list of EventHandler
    """

    # TODO(CD): Base the result value on configuration options.
    return [JsonFileEventWriter("events.txt")]


# Static singleton event bus instance
_event_bus = None


def initialize_event_bus(current_time):
    """Creates a global event bus instance. It is an error to invoke this
    function more than once.
    """

    global _event_bus
    if _event_bus is not None:
        raise
    _event_bus = EventBus(_get_event_handlers({}), current_time)


def get_event_bus():
    """Returns the configured event bus instance.
    The function `initialize_event_bus` must be called before `get_event_bus`.

    :rtype: EventBus
    """

    global _event_bus
    if _event_bus is None:
        raise
    return _event_bus


###############################################################################
# T E S T S
###############################################################################


class TestEventBus(unittest.TestCase):

    def current_time(self):
        if not hasattr(self, "test_time"):
            self.test_time = 0
        result = self.test_time
        self.test_time += 1
        return result

    def lifted_current_time(self):
        return lambda: self.current_time()

    def test_initialize(self):
        initialize_event_bus(self.lifted_current_time())
        event_bus = get_event_bus()
        self.assertTrue(isinstance(event_bus, EventBus))

    def test_publish(self):
        class TestHandler(EventHandler):
            def __init__(self):
                self.events = []

            def handle(self, event):
                self.events.append(event)

        class TestSource(EventSource):
            def __init__(self, event_bus):
                self.event_bus = event_bus

            def name(self):
                return "test_source_name"

            def id(self):
                return "test_source_id"

            def push_event(self, name, data):
                event_bus.publish(self, name, data)

        handler = TestHandler()
        event_bus = EventBus([handler], self.lifted_current_time())
        source = TestSource(event_bus)
        source.push_event("test_event", "test_data_0")
        source.push_event("test_event", "test_data_1")
        source.push_event("test_event", "test_data_2")
        self.assertTrue(len(handler.events) is 3)
        expected0 = {
            "name": "test_event",
            "data": "test_data_0",
            "source": "test_source_name",
            "id": "test_source_id",
            "time": 0,
        }
        self.assertTrue(handler.events[0] == expected0)


if __name__ == '__main__':
    unittest.main()
