import logging as log
import queue
import threading
import time
from datetime import datetime

import Adafruit_BBIO.GPIO as GPIO

import settings as s
from netcat import Netcat


class WorkerTimeoutException(Exception):
    """Exception raised when we timed out waiting for a worker to do something"""
    pass


class Worker:
    """
    Base class for workers
    """

    def __init__(self, id, pin) -> None:
        self.id = id
        # self.pin is the last octet of the MAC address in VM mode
        self.pin = pin
        self._pin_lock = threading.Lock()
        self.job_queue = queue.Queue()
        self.last_connection = datetime.min
        self.last_completion = datetime.min
        self._power_up_thread = None
        self._power_up_thread_terminate = False

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

    def __repr__(self) -> str:
        return "Worker" + str(self.id)


class BBBWorker(Worker):
    def __init__(self, id, pin) -> None:
        super.__init__(id, pin)

        with self._pin_lock:
            log.debug("Setting pin %s to output HIGH for %s", self.pin, self)
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.HIGH)

    def power_up(self, wait_for_connection=True):
        """
        Power up a BBBWorker by pulsing its PWR_BUT line low for BTN_PRESS_DELAY sec
        """

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

    def __repr__(self) -> str:
        return "BBBWorker" + str(self.id)


class VMWorker(Worker):
    def power_up(self, wait_for_connection=True):
        """
        Power up a VMWorker by sending a command to the NC server
        """

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
                log.debug("Sending nc command: " + kvm_cmd)
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

    def __repr__(self) -> str:
        return "VMWorker" + str(self.id)
