#!/usr/bin/env python3
from os import write
import socket
import threading
import socketserver
import queue
import time
import random
import string
import json
import csv
import logging as log
from datetime import datetime, timedelta
from typing_extensions import runtime
import Adafruit_BBIO.GPIO as GPIO

# TCP Server Setup
# Host "" means bind to all interfaces
# Port 0 means to select an arbitrary unused port
HOST, PORT = "", 63302


class Worker:
    BTN_PRESS_DELAY = 0.5
    LAST_CONNECTION_TIMEOUT = datetime.timedelta(seconds=8)
    POWER_UP_MAX_RETRIES = 6

    def __init__(self, id, pin) -> None:
        self.id = id
        self.pin = pin
        self._pin_lock = threading.Lock()
        self.job_queue = queue.Queue()
        self.last_connection = datetime.min
        self.last_completion = datetime.min
        self._power_up_thread = None
        self._power_up_thread_terminate = False

        # Setup GPIO lines
        with self._pin_lock:
            log.debug("Setting pin %s to output HIGH for worker %s", self.pin, self.id)
            GPIO.setup(pin, GPIO.OUT)
            GPIO.output(pin, GPIO.HIGH)

    def power_up(self, wait_for_connection=True):
        """
        Power up a worker by pulsing its PWR_BUT line low for 500ms
        """
        # This "with" statement blocks until it can acquire the pin lock
        log.debug("Worker %s attempting to acquire pin lock on %s", self.id, self.pin)
        with self._pin_lock:
            log.debug("Worker %s acquired pin lock on %s", self.id, self.pin)
            retries = 0
            first_attempt_time = datetime.now()
            while retries < self.POWER_UP_MAX_RETRIES:
                log.info(
                    "Attempting to power up worker %s (try #%d)", self.id, self.pin
                )
                GPIO.output(self.pin, GPIO.LOW)
                time.sleep(self.BTN_PRESS_DELAY)
                GPIO.output(self.pin, GPIO.HIGH)

                if wait_for_connection:
                    log.debug(
                        "Waiting for post-boot connection from worker %s", self.id
                    )
                    time.sleep(self.LAST_CONNECTION_TIMEOUT.seconds)
                    if self.last_connection < first_attempt_time:
                        log.warning(
                            "No post-boot connection from worker %s since %s, retrying...",
                            self.id,
                            self.last_connection,
                        )
                        retries += 1
                        continue
                    else:
                        log.debug(
                            "Successful post-boot connection from worker %s", self.id
                        )
                        return
                else:
                    # No waiting requested, so just return
                    return

            # Reaching this point means we ran out of retries
            log.warning(
                "No connection from worker %s after %d attempts. Giving up.",
                self.id,
                retries,
            )
        return

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
            log.warning(
                "Waiting to acquire pin lock for worker %s (this is unusual)", self.id
            )
            # There's a thread already running. Join it
            self.power_up_thread.join()
            # Now create our own
            self.power_up_thread = threading.Thread(
                target=self.power_up, args=(wait_for_connection,)
            )
            self.power_up_thread.start()
        else:
            # There's a thread already running and user doesn't want to wait for it
            raise RuntimeError("Unable to obtain pin lock for worker " + str(self.id))


class ThreadsafeCSVWriter:
    def __init__(self, path="microfaas-log.csv") -> None:
        self._file_handle = open(path, "a+t")
        self._file_lock = threading.Lock()
        self._writer = csv.DictWriter(
            self._file_handle,
            fieldnames=[
                "invocation_id",
                "worker",
                "function_id",
                "result",
                "init_time",
                "pre_exec_time",
                "post_exec_time",
                "fin_time",
            ],
        )

        with self._file_lock:
            self._writer.writeheader()

    def __del__(self):
        with self._file_lock:
            self._file_handle.close()

    def save_raw_result(self, worker_id, data_json):
        """
        Process and save a raw JSON string recv'd from a worker to CSV
        """

        # data_json should look like {i_id, f_id, result, timing: {init, pre_exec, post_exec, fin_timestamp}}
        # where init, pre_exec, and post_exec are negative millisecond values, and
        # fin_timestamp is a UNIX timestamp in milliseconds to be used as a reference
        data = json.loads(data_json)

        # Convert relative timestamps to absolutes, and milliseconds to fractional seconds
        try:
            tref = int(data["fin_timestamp"])
            row = {
                "invocation_id": data["i_id"],
                "worker": worker_id,
                "function_id": data["f_id"],
                "result": data["result"],
                "init_time": (tref + int(data["init"])) / 1000,
                "pre_exec_time": (tref + int(data["pre_exec"])) / 1000,
                "post_exec_time": (tref + int(data["post_exec"])) / 1000,
                "fin_time": tref / 1000,
            }
        except IndexError:
            log.error("Bad schema")
            return False
        except ValueError:
            log.error("Bad cast")
            return False

        with self._file_lock:
            self._writer.writerow(row)

        return row['invocation_id']

# Mapping of worker IDs to GPIO lines
# We assume the ID# also maps to the last octet of the worker's IP
# e.g., if the orchestrator is 192.168.1.1, and workers are 192.168.1.2-11, this should be range(2, 12)
WORKERS = {
    "2": Worker(2, "P9_41"),
    "3": Worker(3, "P8_7"),
    "4": Worker(4, "P8_8"),
    "5": Worker(5, "P8_9"),
    "6": Worker(6, "P8_10"),
    "7": Worker(7, "P8_11"),
    "8": Worker(8, "P8_12"),
    "9": Worker(9, "P8_14"),
    "10": Worker(10, "P8_15"),
    "11": Worker(11, "P8_16"),
}

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 1000

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 0.5

# JSON payload to send when we want the worker to power down or reboot
SHUTDOWN_PAYLOAD = json.dumps(
    {
        "i_id": "PWROFF",
        "f_id": "fwrite",
        "f_args": {"path": "/proc/sysrq-trigger", "data": "o"},
    }
)
REBOOT_PAYLOAD = json.dumps(
    {
        "i_id": "REBOOT",
        "f_id": "fwrite",
        "f_args": {
            "path": "/proc/sysrq-trigger",
            "data": "b",
        },
    }
)
# SHUTDOWN_PAYLOAD = b"{\"i_id\": \"PWROFF\", \"f_id\": \"fwrite\", \"f_args\": {path}}\n"

# Socket timeout
SOCK_TIMEOUT = 60

# Supported workload functions and sample inputs.
# Make sure COMMANDS.keys() matches your workers' FUNCTIONS.keys()!
random.seed(
    "MicroFaaS", version=2
)  # Hardcode seed for reproducibilityqueues[str(self.worker_id)]
COMMANDS = {
    "float_operations": [{"n": random.randint(1, 1000000)} for _ in range(10)],
    "cascading_sha256": [
        {  # data is 64 random chars, rounds is rand int upto 1 mil
            "data": "".join(random.choices(string.ascii_letters + string.digits, k=64)),
            "rounds": random.randint(1, 1000000),
        }
        for _ in range(10)
    ],
    "cascading_md5": [
        {  # data is 64 random chars, rounds is rand int upto 1 mil
            "data": "".join(random.choices(string.ascii_letters + string.digits, k=64)),
            "rounds": random.randint(1, 1000000),
        }
        for _ in range(10)
    ],
}
random.seed()  # Reset seed to "truly" random


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Set the timeout for blocking socket operations
        self.request.settimeout(SOCK_TIMEOUT)

        # First check if worker identified itself
        log.debug("Incoming request from %s", self.client_address[0])
        try:
            # If first few bytes can be casted to an int, assume it's an ID
            self.worker_id = int(self.request.recv(4).strip())
        except ValueError:
            # Otherwise try to identify the worker by its IP address
            try:
                self.worker_id = int(self.client_address[0].split(".")[-1])
            except ValueError:
                log.error(
                    "Could not deduce worker ID for %s. Dropping request.",
                    self.client_address[0],
                )
                return

        # Record this connection on the Worker object
        try:
            w = WORKERS[str(self.worker_id)]
            w.last_connection = datetime.now()
        except IndexError:
            log.error("Worker with unknown ID %s attempted to connect", self.worker_id)
            return

        while not w.job_queue.empty():
            # Send the worker the next item on the queue
            try:
                job_json = w.job_queue.get_nowait()
                self.request.sendall((job_json + "\n").encode(encoding="ascii"))
                log.info("Transmitted work to worker %s", self.worker_id)
                log.debug(job_json)
            except queue.Empty:
                # This worker's queue is suddenly empty..., so tell it to shutdown
                self.request.sendall(SHUTDOWN_PAYLOAD)
                log.error(
                    "Worker %s's work went missing, probably due to race condition. Shutdown payload sent.",
                    self.worker_id,
                )
                return

            # Now we wait for work to happen and results to come back
            # The socket timeout will limit how long we wait
            self.data = self.request.recv(8192).strip()

            # Save results to CSV
            log.debug("Worker %s returned: %s", self.worker_id, self.data)
            writer_result = writer.save_raw_result(self.worker_id, self.data)
            if not writer_result:
                log.error("Failed to process results from worker %s!", self.worker_id)
            else:
                log.info("Processed results of invocation %s from worker %s", writer_result, self.worker_id)

            # Check if there's more work for it in its queue
            # If yes, send reboot. Otherwise send shutdown
            if w.job_queue.empty():
                self.request.sendall(SHUTDOWN_PAYLOAD)
                return
            else:
                self.request.sendall(REBOOT_PAYLOAD)

        # Reaching here means a worker's queue has been empty since the connection began
        self.request.sendall(SHUTDOWN_PAYLOAD)
        log.warning(
            "Worker %s requested work while queue empty. Shutdown payload sent.",
            self.worker_id,
        )


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def server_bind(self) -> None:
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        return


def load_generator(count):
    """
    Load generation thread. Run as daemon

    Currently a stub that just gives every queue a function each period
    """
    while count > 0:
        for _, w in WORKERS.items():
            q_was_empty = w.job_queue.empty()
            f_id = random.choice(list(COMMANDS.keys()))
            cmd = {
                # Invocation ID
                "i_id": "".join(
                    random.choices(string.ascii_letters + string.digits, k=6)
                ),
                # Function ID (one of COMMANDS.keys())
                "f_id": f_id,
                # Function arguments
                "f_args": random.choice(COMMANDS[f_id]),
            }
            w.job_queue.put_nowait(json.dumps(cmd))
            log.debug("Added job to worker %s's queue: %s", w.id, json.dumps(cmd))
            if q_was_empty:
                # This worker's queue was empty, meaning it probably isn't
                # powered on right now. Now that it has work, power it up
                # asynchronously (so that this thread can continue)
                log.debug("Requesting async power-up of worker %s", w.id)
                w.power_up_async()

            count -= 1
        time.sleep(LOAD_GEN_PERIOD)


if __name__ == "__main__":

    # Set up CSV writer
    writer = ThreadsafeCSVWriter(datetime.strftime("%Y%m%d.%H%M%S")+".microfaas-log.csv")

    # Set up load generation thread
    load_gen_thread = threading.Thread(
        target=load_generator, daemon=True, args=(FUNC_EXEC_COUNT,)
    )
    load_gen_thread.start()

    # Set up server thread
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()
        print("Server loop running in thread:", server_thread.name)

        # client(ip, port, 6)
        # client(ip, port, 4)
        # client(ip, port, 5)

        # Run server for an hour (TODO: hacky af)
        time.sleep(3600)

        server.shutdown()
