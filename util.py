import logging as log
from threading import Event, Thread
from time import sleep
from typing import Any, Callable, NoReturn

class FakeGPIO:
    """
    Adafruit GPIO stub class
    """
    # Dummy class variables
    OUT = "OUT"
    IN = "IN"
    HIGH = "HIGH"
    LOW = "LOW"

    def setup(pin, direction):
        log.debug("FakeGPIO: setup pin %s as %s", pin, direction)

    def output(pin, level):
        log.debug("FakeGPIO: output %s on pin %s", level, pin)

class IOEvent(Event):
    """Event with some conveinience methods for usage in a state machine"""

    def __init__(self, id: str) -> None:
        super().__init__()
        self.id = id

    def wait_then_clear(self, timeout: int = None):
        retval = self.wait(timeout)
        self.clear()
        return retval

    def __repr__(self) -> str:
        return self.__class__.__name__ + ":" + str(self.id)


class ActionableIOEvent(IOEvent):
    """IOEvent that runs an action in a separate thread upon set()"""

    def __init__(self, id: str, action: Callable[[], Any], holdoff: int = None):
        super().__init__(id)
        self._action = action
        self._holdoff = holdoff
        self._monitor_thread = Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()

    def _monitor(self) -> NoReturn:
        if self._holdoff is not None:
            sleep(self._holdoff)

        while True:
            self.wait()
            try:
                self._action()
                self.clear()
            except Exception as ex:
                log.error("Action for %s threw exception: %s", self, ex)
                sleep(5)


class IOEventGroup():
    def __init__(self, id: str) -> None:
        self.id = id

    def __repr__(self) -> str:
        return self.__class__.__name__ + str(self.id)

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value