from enum import Enum, auto, unique
import json
import logging as log
import queue
import threading
import time
from datetime import datetime, timedelta

import Adafruit_BBIO.GPIO as GPIO # type: ignore

import settings as s
from netcat import Netcat


class WorkerTimeoutException(Exception):
    """Exception raised when we timed out waiting for a worker to do something"""
    pass


class WorkerHoldoffException(Exception):
    """Exception raised when we asked a worker to do something before the holdoff period expired"""
    pass

class WorkerEvent(threading.Event):
    """Event with some conveinience methods for usage in a state machine"""
    def wait_then_clear(self, timeout = None):
        retval = self.wait(timeout)
        self.clear()
        return retval

class ActionableWorkerEvent(WorkerEvent):
    """WorkerEvent that runs an action in a separate thread upon set()"""
    def __init__(self, action, holdoff = None):
        self._action = action
        self._holdoff = holdoff
        self._monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()

    def _monitor(self):
        if self._holdoff is not None:
            time.sleep(self._holdoff)

        while True:
            self.wait_then_clear()
            self._action()
        

@unique
class WorkerState(Enum):
    UNKNOWN = auto()
    POWERING_UP = auto()
    WORKING = auto()
    REBOOTING = auto()
    OFF = auto()

class IterMixin(object):
    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

class InputEvents(IterMixin):
    def __init__(self) -> None:
        self.WORKER_REQUEST = WorkerEvent()
        self.QUEUE_NOT_EMPTY = WorkerEvent()

class OutputEvents(IterMixin):
    def __init__(self, power_up_action, power_up_holdoff, power_down_action, power_down_holdoff) -> None:
        self.DEQUEUE = WorkerEvent()
        self.REBOOT = WorkerEvent()
        self.POWER_UP = ActionableWorkerEvent(power_up_action, power_up_holdoff)
        self.POWER_DOWN = ActionableWorkerEvent(power_down_action, power_down_holdoff)
    def clear_all(self):
        for _, ev in self:
            ev.clear()

class Worker:
    """
    Base class for workers
    """

    def __init__(self, id, pin, power_up_holdoff, power_down_holdoff) -> None:
        # State Machine I/O
        self._I = InputEvents()
        self._O = OutputEvents(self.power_up, power_up_holdoff, self.power_down)

        self.cycle_counts = {}
        for event in self._I.keys() + self._O.keys():
            self.cycle_counts[event] = 0

        self.id = id
        # self.pin is the last octet of the MAC address in VM mode
        self.pin = pin
        self._pin_lock = threading.Lock()

        self._state = WorkerState.UNKNOWN
        self._state_lock = threading.RLock()

        self._queue_lock = threading.RLock()
        self._job_queue = queue.Queue()

        self.last_connection = datetime.min
        self.last_completion = datetime.min
        self.instantiated = datetime.now()
        self.last_state_change = datetime.now()
        self._job_timeout = timedelta(seconds = 120)

        self._state_machine_thread = None

    def _state_machine(self):
        while True:
            self.cycle_counts[self._state] += 1
            if self.in_state(WorkerState.POWERING_UP):
                self._O.POWER_DOWN.clear() # Just in case we're exiting holdoff
                if self._I.WORKER_REQUEST.wait_then_clear(timeout = 60):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout = 1):
                        self._O.DEQUEUE.set()
                        #self._set_timer()
                        self.set_state(WorkerState.WORKING)
                    else:
                        self._O.POWER_DOWN.set()
                        self.set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self.set_state(WorkerState.UNKNOWN)

            elif self.in_state(WorkerState.WORKING):
                self._O.POWER_DOWN.clear() # Just in case we're exiting holdoff
                if self._I.WORKER_REQUEST.wait_then_clear(timeout = 120):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout = 1):
                        self._O.REBOOT.set()
                        #self._set_timer()
                        self.set_state(WorkerState.REBOOTING)
                    else:
                        self._O.POWER_DOWN.set()
                        self.set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self.set_state(WorkerState.UNKNOWN)
            
            elif self.in_state(WorkerState.REBOOTING):
                self._O.POWER_UP.clear() # Just in case we're exiting holdoff
                self._O.POWER_DOWN.clear() # Ditto
                if self._I.WORKER_REQUEST.wait_then_clear(timeout = 120):
                    if self._I.QUEUE_NOT_EMPTY.wait(timeout = 1):
                        self._O.DEQUEUE.set()
                        self.set_state(WorkerState.WORKING)
                    else:
                        self._O.POWER_DOWN.set()
                        self.set_state(WorkerState.OFF)
                else:
                    # Timeout
                    self.set_state(WorkerState.UNKNOWN)
            
            elif self.in_state(WorkerState.OFF):
                self._O.POWER_UP.clear() # Just in case we're exiting holdoff
                if self._I.QUEUE_NOT_EMPTY.wait(timeout = 1):
                    self._O.POWER_UP.set()
                    self.set_state(WorkerState.POWERING_UP)
                elif self._I.WORKER_REQUEST.wait_then_clear(timeout = 1):
                    # We got a worker request during OFF with an empty queue?
                    self._O.POWER_DOWN.set()
                    self.set_state(WorkerState.OFF)
            
            elif self.in_state(WorkerState.UNKNOWN):
                if self._I.WORKER_REQUEST.wait_then_clear(timeout = 30):
                    self._O.REBOOT.set()
                    self.set_state(WorkerState.REBOOTING)
                else:
                    # Timeout
                    self._O.POWER_UP.set()
                    self.set_state(WorkerState.POWERING_UP)

            else:
                # Uh oh
                log.critical("%s entered undefined state %s. Moving to UNKNOWN", self, self._state)
                self.set_state(WorkerState.UNKNOWN)

    def _set_timer(self):
        self._timer = threading.Timer(120, self._timeout_event.set)

    def _clear_outputs(self):
        for k, event in self._events.items():
            if isinstance(k, O):
                event.clear()   

    def begin(self):
        self._monitor_thread = threading.Thread(target=self._monitor, daemon=True)
        self._monitor_thread.start()

    def in_state(self, state) -> bool:
        """
        Thread-safe state comparator
        """
        with self._state_lock:
            return self._state == state

    def set_state(self, state):
        """
        Thread-safe and timestamped setter for this worker's state
        """
        with self._state_lock:
            self.last_state_change = datetime.now()
            self._state = state

    def enqueue_job(self, job):
        """
        Add a job to this worker's queue

        @throws queue.Full if the queue is full
        """
        with self._queue_lock:
            self._job_queue.put_nowait(job)

    def dequeue_job(self):
        """
        Get the next job from this workers's queue

        @throws queue.Empty if the queue is empty
        """
        with self._queue_lock:
            job = self._job_queue.get_nowait()
            self.set_state(WorkerState.WORKING)
            return job

    def job_complete(self):
        """
        Indicate that the last job (returned by dequeue_job()) is complete

        @throws ValueError if called more times than # of jobs added to queue
        """
        self._job_queue.task_done()
        

    def power_up(self, wait_for_connection=True):
        """
        Power up a worker
        """
        raise NotImplementedError("power_up must be implemented in subclass")

    def power_up_async(self, wait_for_connection=True, block_if_locked=False):
        """
        Power up a worker using a separate thread
        """

        if not self._pin_lock.locked():
            self.power_up_thread = threading.Thread(
                target=self.power_up, args=(wait_for_connection,)
            )
            self.power_up_thread.start()
        elif block_if_locked:
            log.warning("Waiting to acquire pin lock for %s (this is unusual)", self)
            # There's a thread already running. Join it
            self.power_up_thread.join()
            # Now create our own
            self.power_up_thread = threading.Thread(
                target=self.power_up, args=(wait_for_connection,)
            )
            self.power_up_thread.start()
        else:
            # There's a thread already running and user doesn't want to wait for it
            raise RuntimeError("Unable to obtain pin lock for " + str(self))

    def power_down():
        """
        Helper method. May or may not return a payload. You're better off using either
        power_down_externally() or power_down_payload() directly
        """
        try:
            self.power_down_externally()
        except NotImplementedError:
            return self.power_down_payload()

    def power_down_externally():
        """
        Power down worker IF it supports being powered off externally. Otherwise, raises a
        NotImplementedError which should be caught and followed up with a call to 
        power_down_payload()
        """
        raise NotImplementedError()

    def power_down_payload() -> bytes:
        """
        Returns an ASCII-encoded byte string containing the command to be sent over-the-wire to a
        worker instructing it to power itself down IF it supports this. Otherwise, raises a
        NotImplementedError which should be caught and followed up with a call to
        power_down_externally()
        """
        raise NotImplementedError()

    def reboot_payload() -> bytes:
        """
        Returns an ASCII-encoded byte string containing the command to be sent over-the-wire to a
        worker instructing it to reboot itself
        """
        return (json.dumps({
            "i_id": "REBOOT",
            "f_id": "fwrite",
            "f_args": {"path": "/proc/sysrq-trigger", "data": "b"},
        }) + "\n").encode(encoding="ascii")

    def _wait_for_connection(self, first_attempt_time):
        """
        Block until worker connects, or timeout and raise a WorkerTimeoutException
        """
        log.debug("Waiting for post-boot connection from %s", self)
        time.sleep(s.LAST_CONNECTION_TIMEOUT)
        if self.last_connection < first_attempt_time:
            log.warning(
                "No post-boot connection from %s since %s, retrying...", self, self.last_connection
            )
            raise WorkerTimeoutException()

        else:
            log.debug("Successful post-boot connection from %s", self)
            return

    def _holding_off(self, hold_off_duration):
        return datetime.now() - self.instantiated > hold_off_duration

    def _raise_if_holding_off(self, exc, hold_off_duration):
        """Raise an exception if we're currently in a holdoff period"""
        if self._holding_off(hold_off_duration):
            raise exc

    def __repr__(self) -> str:
        return "Worker" + str(self.id)


class BBBWorker(Worker):
    def __init__(self, id, pin) -> None:
        super.__init__(id, pin)

        self._power_up_holdoff = timedelta(seconds = s.POWER_UP_HOLDOFF_BBB)
        self._power_down_holdoff = timedelta(seconds = s.POWER_DOWN_HOLDOFF_BBB)

        with self._pin_lock:
            log.debug("Setting pin %s to output HIGH for %s", self.pin, self)
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.HIGH)

    def power_up(self, wait_for_connection=True):
        """
        Power up a BBBWorker by pulsing its PWR_BUT line low for BTN_PRESS_DELAY sec

        @throws WorkerHoldoffException if the holdoff period has yet to expire
        """
        # Obey power-up holdoff period
        self._raise_if_holding_off(
            WorkerHoldoffException("Too early to power-up {}".format(self)),
            self._power_up_holdoff
        )
        log.debug("%s attempting to acquire pin lock on %s", self, self.pin)
        with self._pin_lock:
            log.debug("%s acquired pin lock on %s", self, self.pin)
            retries = 0
            first_attempt_time = datetime.now()
            while retries < s.POWER_UP_MAX_RETRIES:
                log.info("Attempting to power up %s (try #%d)", self, retries)
                GPIO.output(self.pin, GPIO.LOW)
                time.sleep(s.BTN_PRESS_DELAY)
                GPIO.output(self.pin, GPIO.HIGH)

                if wait_for_connection:
                    # User requested we block until the freshly-booted worker connects
                    try:
                        self._wait_for_connection(first_attempt_time)
                    except WorkerTimeoutException:
                        retries += 1
                        continue
                else:
                    # No waiting requested, so just return
                    return

            # Reaching this point means we ran out of retries
            log.error("No connection from %s after %d attempts. Giving up.", self, retries)
        return

    def power_down_payload(self) -> bytes:
        """
        Returns ASCII-encoded bytes for JSON "PWROFF" command

        @throws WorkerHoldoffException if the holdoff period has yet to expire
        """
        # Obey power-down holdoff period
        self._raise_if_holding_off(
            WorkerHoldoffException("Too early to power-down {}".format(self)),
            self._power_down_holdoff
        )
        # We acquire the lock to prevent sending PWROFF in the midd
        with self._pin_lock:
            return (json.dumps({
                    "i_id": "PWROFF",
                    "f_id": "fwrite",
                    "f_args": {"path": "/proc/sysrq-trigger", "data": "o"},
                }) + "\n").encode(encoding="ascii")

    def __repr__(self) -> str:
        return "BBBWorker" + str(self.id)


class VMWorker(Worker):
    def __init__(self, id, pin) -> None:
        super.__init__(id, pin)

        self._power_up_holdoff = timedelta(seconds = s.POWER_UP_HOLDOFF_VM)
        self._power_down_holdoff = timedelta(seconds = s.POWER_DOWN_HOLDOFF_VM)

    def power_up(self, wait_for_connection=True):
        """
        Power up this VMWorker by sending a kvm command to the NC server.

        @throws WorkerHoldoffException if the holdoff period has yet to expire
        """
        # Obey power-up holdoff period
        self._raise_if_holding_off(
            WorkerHoldoffException("Too early to power-up {}".format(self)),
            self._power_up_holdoff
        )
        log.debug("%s attempting to acquire pin lock on %s", self, self.pin)
        with self._pin_lock:
            log.debug("%s acquired pin lock on %s", self, self.pin)
            retries = 0
            first_attempt_time = datetime.now()
            while retries < s.POWER_UP_MAX_RETRIES:
                log.info("Attempting to power up %s (try #%d)", self, retries)

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
                nc.write((kvm_cmd + " \n").encode())
                nc.close()

                if wait_for_connection:
                    # User requested we block until the freshly-booted worker connects
                    try:
                        self._wait_for_connection(first_attempt_time)
                    except WorkerTimeoutException:
                        retries += 1
                        continue
                else:
                    # No waiting requested, so just return
                    return

            # Reaching this point means we ran out of retries
            log.error("No connection from %s after %d attempts. Giving up.", self, retries)
        return

    def power_down_externally(self):
        """
        Powers down this VMWorker by sending a pkill command to the NC server, BLOCKING if
        necessary (e.g., because a power-up command is currently in progress)

        @throws WorkerHoldoffException if the holdoff period has yet to expire
        """
        # Obey power-down holdoff period
        self._raise_if_holding_off(
            WorkerHoldoffException("Too early to power-down {}".format(self)),
            self._power_down_holdoff
        )
        with self._pin_lock:
            log.debug("Sending pkill to %s", self)
            nc = Netcat(s.NC_IP, s.NC_PORT)
            poweroff_cmd = "pkill -of \"" + self.pin + "\"\n"
            nc.write(poweroff_cmd.encode(encoding="ascii"))
            nc.close()

    def __repr__(self) -> str:
        return "VMWorker" + str(self.id)
