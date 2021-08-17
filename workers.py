import json
import logging as log
import queue
import threading
from time import sleep
from typing import Any, Callable, NoReturn, Optional
from datetime import datetime, timedelta
from enum import Enum, auto

try:
    import Adafruit_BBIO.GPIO as GPIO  # type: ignore
except ModuleNotFoundError:
    log.warning("Adafruit GPIO module not found, stubbing with FakeGPIO")
    from util import FakeGPIO as GPIO

import settings as s
from netcat import Netcat
from util import ActionableIOEvent, IOEvent, IOEventGroup


class WorkerState(Enum):
    """Enum for states of a Worker's internal state machine"""

    UNKNOWN = auto()
    POWERING_UP = auto()
    WORKING = auto()
    REBOOTING = auto()
    OFF = auto()


class InputEvents(IOEventGroup):
    """Inputs to a Worker's internal state machine"""

    def __init__(self, id: str):
        super().__init__(id)
        self.WORKER_REQUEST = IOEvent(id + ":WORKER_REQUEST")
        self.QUEUE_NOT_EMPTY = IOEvent(id + ":QUEUE_NOT_EMPTY")


class OutputEvents(IOEventGroup):
    """Outputs from a Worker's internal state machine"""

    def __init__(
        self,
        id: str,
        power_up_action: Callable[[], Any],
        power_up_holdoff: int,
        power_down_action: Callable[[], None],
        power_down_holdoff: int,
    ):
        # DEQUEUE and REBOOT only control fetched payloads, so they don't need to be Actionable
        self.DEQUEUE = IOEvent(id + ":DEQUEUE")
        self.REBOOT = IOEvent(id + ":REBOOT")
        self.POWER_UP = ActionableIOEvent(id + ":POWER_UP", power_up_action, power_up_holdoff)
        self.POWER_DOWN = ActionableIOEvent(id + ":POWER_DOWN", power_down_action, power_down_holdoff)


class Worker:
    """
    Base class for workers
    """

    def __init__(self, id: int, pin: str, power_up_holdoff: int, power_down_holdoff: int):
        # Basics
        # ID of the worker is assumed to be last octet its IPv4 address
        self.id = id
        self._active = False
        self._job_queue = queue.Queue()

        # Internal state machine
        self._state = WorkerState.UNKNOWN
        self._state_machine_thread = None
        
        # State machine I/O
        self._I = InputEvents(str(self) + "-I")
        self._O = OutputEvents(
            str(self) + "-O", self._power_up, power_up_holdoff, self._power_down, power_down_holdoff
        )

        # Track how long the state machine is in each state
        self.cycle_counts = {}
        for st in WorkerState:
            self.cycle_counts[st.name] = 0

        # "pin" identifier that power state control functions will access
        self.pin = pin
        self._pin_lock = threading.Lock()

    def activate(self) -> None:
        """
        Start this Worker's state machine monitoring thread. 
        """
        self._state_machine_thread = threading.Thread(target=self._state_machine, daemon=True)
        self._active = True
        self._state_machine_thread.start()

    def deactivate(self, join: bool = False) -> None:
        """
        Stop this Worker's state machine monitoring thread, optionally blocking until the thread
        terminates.

        @raises RuntimeError if state machine thread doesn't stop within 120 seconds
        """
        self._active = False
        if join:
            try:
                self._state_machine_thread.join(timeout=120)
            except AttributeError:
                pass
            if self._state_machine_thread.is_alive():
                raise RuntimeError("Failed to stop state machine before timeout")


    def is_active(self) -> bool:
        """
        Returns True if this worker's state machine monitor is alive
        """
        return self._active and self._state_machine_thread.is_alive()
 
    def _state_machine(self) -> NoReturn:
        while self._active:
            self.cycle_counts[self._state.name] += 1

            if self.in_state(WorkerState.POWERING_UP):
                self._O.POWER_DOWN.clear()  # Just in case we're exiting holdoff
                if self._I.WORKER_REQUEST.wait_then_clear(timeout=60):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout=1):
                        self._O.DEQUEUE.set()
                        self._set_state(WorkerState.WORKING)
                    else:
                        self._O.POWER_DOWN.set()
                        self._set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self._O.POWER_UP.set()
                    self._set_state(WorkerState.POWERING_UP)

            elif self.in_state(WorkerState.WORKING):
                self._O.POWER_DOWN.clear()  # Just in case we're exiting holdoff
                if self._I.WORKER_REQUEST.wait_then_clear(timeout=120):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout=1):
                        self._O.REBOOT.set()
                        self._set_state(WorkerState.REBOOTING)
                    else:
                        self._O.POWER_DOWN.set()
                        self._set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self._set_state(WorkerState.UNKNOWN)

            elif self.in_state(WorkerState.REBOOTING):
                self._O.POWER_UP.clear()  # Just in case we're exiting holdoff
                self._O.POWER_DOWN.clear()  # Ditto
                if self._I.WORKER_REQUEST.wait_then_clear(timeout=120):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout=1):
                        self._O.DEQUEUE.set()
                        self._set_state(WorkerState.WORKING)
                    else:
                        self._O.POWER_DOWN.set()
                        self._set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self._set_state(WorkerState.UNKNOWN)

            elif self.in_state(WorkerState.OFF):
                self._O.POWER_UP.clear()  # Just in case we're exiting holdoff
                if self._I.QUEUE_NOT_EMPTY.wait(timeout=1):
                    self._O.POWER_UP.set()
                    self._set_state(WorkerState.POWERING_UP)
                elif self._I.WORKER_REQUEST.wait_then_clear(timeout=1):
                    # We got a worker request during OFF with an empty queue?
                    self._O.POWER_DOWN.set()
                    self._set_state(WorkerState.OFF)

            elif self.in_state(WorkerState.UNKNOWN):
                if self._I.WORKER_REQUEST.wait_then_clear(timeout=30):
                    self._O.REBOOT.set()
                    self._set_state(WorkerState.REBOOTING)
                else:
                    # Timeout
                    self._O.POWER_UP.set()
                    self._set_state(WorkerState.POWERING_UP)

            else:
                # Uh oh
                log.critical("%s entered undefined state %s. Moving to UNKNOWN", self, self._state)
                self._set_state(WorkerState.UNKNOWN)

    def handle_worker_request(self) -> Optional[bytes]:
        """
        Called from outside the class to indicate that this worker is requesting its next job.
        Triggers the WORKER_REQUEST state machine input. May return a payload if appropriate

        @returns payload if appropriate, otherwise None
        """
        self._I.WORKER_REQUEST.set()
        # Block until flag acknowledged and cleared by monitor thread
        while self._I.WORKER_REQUEST.is_set():
            sleep(0.1)

        # Now check relevant outputs
        return_payload = None
        if self._O.DEQUEUE.is_set():
            self._O.DEQUEUE.clear()
            try:
                return_payload = self._dequeue_job()
            except queue.Empty:
                log.warning("%s requested job while queue empty", self)
        elif self._O.REBOOT.is_set():
            self._O.REBOOT.clear()
            return_payload = self.reboot_payload()
        elif self._O.POWER_DOWN.is_set():
            self._O.POWER_DOWN.clear()
            try:
                return self.power_down_payload()
            except NotImplementedError:
                # Worker only powers down externally, so payload useless here
                log.debug("%s should be powering off externally", self)
        elif self._O.POWER_UP.is_set():
            # Getting a worker request means we don't need POWER_UP to be set
            self._O.POWER_UP.clear()
            # We don't need to do anything else, assuming the monitor takes over here
        else:
            log.warning("%s made request but no output events set", self)

        return return_payload

    def in_state(self, state) -> bool:
        """
        Comparator for this worker's state
        """
        return self._state == state

    def _set_state(self, state) -> None:
        """
        Setter for this worker's state
        """
        self._state = state

    def enqueue_job(self, job: str) -> None:
        """
        Add a job to this worker's queue

        @throws queue.Full if the queue is full
        """
        self._job_queue.put_nowait(job)
        self._I.QUEUE_NOT_EMPTY.set()

    def _dequeue_job(self) -> bytes:
        """
        Get the next job from this workers's queue

        @returns next job as ASCII-encoded JSON byte string
        @throws queue.Empty if queue empty
        """
        try:
            return self._job_queue.get_nowait().encode(encoding="ascii")
        except queue.Empty as ex:
            self._I.QUEUE_NOT_EMPTY.clear()
            raise ex

    def _power_up(self) -> None:
        """
        Power up a worker
        """
        raise NotImplementedError("power_up must be implemented in subclass")

    def _power_down(self) -> Optional[bytes]:
        """
        Helper method. May or may not return a payload. You're better off using either
        power_down_externally() or power_down_payload() directly
        """
        try:
            self._power_down_externally()
        except NotImplementedError:
            return self.power_down_payload()

    def _power_down_externally(self) -> None:
        """
        Power down worker IF it supports being powered off externally. Otherwise, raises a
        NotImplementedError which should be caught and followed up with a call to
        power_down_payload()
        """
        raise NotImplementedError()

    def power_down_payload(self) -> bytes:
        """
        Returns an ASCII-encoded byte string containing the command to be sent over-the-wire to a
        worker instructing it to power itself down IF it supports this. Otherwise, raises a
        NotImplementedError which should be caught and followed up with a call to
        power_down_externally()
        """
        raise NotImplementedError()

    def reboot_payload(self) -> bytes:
        """
        Returns an ASCII-encoded byte string containing the command to be sent over-the-wire to a
        worker instructing it to reboot itself
        """
        return (
            json.dumps(
                {
                    "i_id": "REBOOT",
                    "f_id": "fwrite",
                    "f_args": {"path": "/proc/sysrq-trigger", "data": "b"},
                }
            )
            + "\n"
        ).encode(encoding="ascii")

    def __repr__(self) -> str:
        return self.__class__.__name__ + str(self.id)


class BBBWorker(Worker):
    def __init__(self, id: int, pin: str):
        super().__init__(id, pin, s.POWER_UP_HOLDOFF_BBB, s.POWER_DOWN_HOLDOFF_BBB)

        self._power_up_holdoff = timedelta(seconds=s.POWER_UP_HOLDOFF_BBB)
        self._instantiated = datetime.now()

        with self._pin_lock:
            log.debug("Setting pin %s to output HIGH for %s", self.pin, self)
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.HIGH)

    def _power_up(self) -> None:
        """
        Power up a BBBWorker by pulsing its PWR_BUT line low for BTN_PRESS_DELAY sec. Should only
        be called by the ActionableIOEvent, which will enforce holdoff periods
        """
        log.debug("%s attempting to acquire pin lock on %s", self, self.pin)
        with self._pin_lock:
            log.info("Attempting to power up %s", self)
            GPIO.output(self.pin, GPIO.LOW)
            sleep(s.BTN_PRESS_DELAY)
            GPIO.output(self.pin, GPIO.HIGH)

    def power_down_payload(self) -> bytes:
        """
        Returns ASCII-encoded bytes for JSON "PWROFF" command IF holdoff period expired. Otherwise
        returns reboot payload.
        """
        # This func. isn't called by an ActionableIOEvent, so enforce holdoffs ourselves
        if datetime.now() - self._instantiated < self._power_down_holdoff:
            with self._pin_lock:
                return (
                    json.dumps(
                        {
                            "i_id": "PWROFF",
                            "f_id": "fwrite",
                            "f_args": {"path": "/proc/sysrq-trigger", "data": "o"},
                        }
                    )
                    + "\n"
                ).encode(encoding="ascii")
        else:
            return self.reboot_payload()


class VMWorker(Worker):
    def __init__(self, id: int, pin: str) -> None:
        super().__init__(id, pin, s.POWER_UP_HOLDOFF_VM, s.POWER_DOWN_HOLDOFF_VM)

    def _power_up(self) -> None:
        """
        Power up this VMWorker by sending a kvm command to the NC server. Should only
        be called by the ActionableIOEvent, which will enforce holdoff periods
        """
        with self._pin_lock:
            log.info("Attempting to power up %s", self)
            nc = Netcat(s.NC_IP, s.NC_PORT)
            mac_addr = "DE:AD:BE:EF:00" + self.pin
            boot_args = (
                "ip=192.168.1."
                + str(self.id)
                + "::192.168.1.1:255.255.255.0:worker"
                + str(self.id)
                + ":eth0:off:1.1.1.1:8.8.8.8:209.50.63.74 "
                + " reboot=t quiet loglevel=0 root=/dev/ram0 rootfstype=ramfs rdinit=/sbin/init console=ttyS0"
            )
            kvm_cmd = (
                ' kvm -M microvm -vga none -no-user-config -nographic -kernel bzImage  -append "'
                + boot_args
                + '" -netdev tap,id=net0,script=bin/ifup.sh,downscript=bin/ifdown.sh    -device virtio-net-device,netdev=net0,mac='
                + mac_addr
                + " &"
            )
            log.debug("Sending nc command %s to power-up %s", kvm_cmd, self)
            nc.write((kvm_cmd + " \n").encode(encoding="ascii"))
            nc.close()

    def _power_down_externally(self) -> None:
        """
        Powers down this VMWorker by sending a pkill command to the NC server, BLOCKING if
        necessary (e.g., because a power-up command is currently in progress). Should only
        be called by the ActionableIOEvent, which will enforce holdoff periods
        """
        with self._pin_lock:
            log.debug("Sending pkill to %s", self)
            nc = Netcat(s.NC_IP, s.NC_PORT)
            poweroff_cmd = 'pkill -of "' + self.pin + '"\n'
            nc.write(poweroff_cmd.encode(encoding="ascii"))
            nc.close()
