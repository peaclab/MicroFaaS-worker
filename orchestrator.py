#!/usr/bin/env python3
from json.decoder import JSONDecodeError
import os
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
from zlib import compress
from binascii import hexlify
from numpy import random as nprand
from datetime import datetime, timedelta
import Adafruit_BBIO.GPIO as GPIO

# TCP Server Setup
# Host "" means bind to all interfaces
# Port 0 means to select an arbitrary unused port
HOST, PORT = "", 63302

log.basicConfig(level=log.INFO)

class Worker:
    BTN_PRESS_DELAY = 0.5
    LAST_CONNECTION_TIMEOUT = timedelta(seconds=8)
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
                    "Attempting to power up worker %s (try #%d)", self.id, retries
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
                "begin_exec_time",
                "end_exec_time",
                "fin_time",
            ],
        )

        with self._file_lock:
            self._writer.writeheader()
            self._file_handle.flush()
            os.fsync(self._file_handle)

    def __del__(self):
        with self._file_lock:
            self._file_handle.flush()
            os.fsync(self._file_handle)
            self._file_handle.close()

    def save_raw_result(self, worker_id, data_json):
        """
        Process and save a raw JSON string recv'd from a worker to CSV
        """

        # data_json should look like {i_id, f_id, result, timing: {init, begin_exec, end_exec, fin_timestamp}}
        # where init, pre_exec, and post_exec are negative millisecond values, and
        # fin_timestamp is a UNIX timestamp in milliseconds to be used as a reference
        try:
            data = json.loads(data_json)
        except JSONDecodeError as e:
            log.error("Cannot save malformed JSON from worker %s: %s", worker_id, e)
            return False

        # Convert relative timestamps to absolutes, and milliseconds to fractional seconds
        try:
            tref = int(data["timing"]["fin_timestamp"])
            row = {
                "invocation_id": data["i_id"],
                "worker": worker_id,
                "function_id": data["f_id"],
                "result": data["result"],
                "init_time": (tref + int(data["timing"]["init"])) / 1000,
                "begin_exec_time": (tref + int(data["timing"]["begin_exec"])) / 1000,
                "end_exec_time": (tref + int(data["timing"]["end_exec"])) / 1000,
                "fin_time": tref / 1000,
            }
        except KeyError as e:
            log.error("Bad schema: %s", e)
            return False
        except ValueError as e:
            log.error("Bad cast: %s", e)
            return False

        with self._file_lock:
            self._writer.writerow(row)
            self._file_handle.flush()
            os.fsync(self._file_handle)

        return row['invocation_id']

# Mapping of worker IDs to GPIO lines
# We assume the ID# also maps to the last octet of the worker's IP
# e.g., if the orchestrator is 192.168.1.1, and workers are 192.168.1.2-11, this should be range(2, 12)
WORKERS = {
    "2": Worker(2, "P9_41"),
    "3": Worker(3, "P8_7"),
    # "4": Worker(4, "P8_8"),
    # "5": Worker(5, "P8_9"),
    # "6": Worker(6, "P8_10"),
    # "7": Worker(7, "P8_11"),
    # "8": Worker(8, "P8_12"),
    # "9": Worker(9, "P8_14"),
    # "10": Worker(10, "P8_15"),
    # "11": Worker(11, "P8_16"),
}

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 1000

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 10

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
# Hardcode seeds for reproducibility
random.seed("MicroFaaS", version=2)
nprand.seed(63302)
matrix_sizes = list([random.randint(2, 10) for _ in range(10)])
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
    "matmul": [
        {
            "A": nprand.random((matrix_sizes[n], matrix_sizes[n])).tolist(),
            "B": nprand.random((matrix_sizes[n], matrix_sizes[n])).tolist()
        } for n in range(10)
    ],
    "linpack": [
        {
            "A": nprand.random((matrix_sizes[n], matrix_sizes[n])).tolist(),
            "B": nprand.random((matrix_sizes[n], )).tolist()
        } for n in range(10)
    ],
    "html_generation": [{"n": random.randint(1, 128)} for _ in range(10)],
    "pyaes": [
        {  # data is 16*n random chars, rounds is rand int upto 10k
            "data": "".join(random.choices(string.ascii_letters + string.digits, k=random.randint(1,10)*16)),
            "rounds": random.randint(1, 10000),
        }
        for _ in range(10)
    ],
    "zlib_decompress": [ # Having a little fun here, as random strings don't compress well
        {"data": hexlify(compress(b"It was the best of times.\nIt was the worst of times.")).decode("ascii")},
        {"data": hexlify(compress(b"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.")).decode("ascii")},
        {"data": hexlify(compress(b"But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete account of the system, and expound the actual teachings of the great explorer of the truth, the master-builder of human happiness. No one rejects, dislikes, or avoids pleasure itself, because it is pleasure, but because those who do not know how to pursue pleasure rationally encounter consequences that are extremely painful. Nor again is there anyone who loves or pursues or desires to obtain pain of itself, because it is pain, but because occasionally circumstances occur in which toil and pain can procure him some great pleasure. To take a trivial example, which of us ever undertakes laborious physical exercise, except to obtain some advantage from it? But who has any right to find fault with a man who chooses to enjoy a pleasure that has no annoying consequences, or one who avoids a pain that produces no resultant pleasure?")).decode("ascii")},
        {"data": hexlify(compress(b"We hold these truths to be self-evident, that all men are created equal, that they are endowed by their Creator with certain unalienable Rights, that among these are Life, Liberty and the pursuit of Happiness.--That to secure these rights, Governments are instituted among Men, deriving their just powers from the consent of the governed, --That whenever any Form of Government becomes destructive of these ends, it is the Right of the People to alter or to abolish it, and to institute new Government, laying its foundation on such principles and organizing its powers in such form, as to them shall seem most likely to effect their Safety and Happiness. Prudence, indeed, will dictate that Governments long established should not be changed for light and transient causes; and accordingly all experience hath shewn, that mankind are more disposed to suffer, while evils are sufferable, than to right themselves by abolishing the forms to which they are accustomed.")).decode("ascii")},
        {"data": hexlify(compress(b"Do not go gentle into that good night,\nOld age should burn and rave at close of day;\nRage, rage against the dying of the light.\n\nThough wise men at their end know dark is right,\nBecause their words had forked no lightning they\nDo not go gentle into that good night.\nGood men, the last wave by, crying how bright\nTheir frail deeds might have danced in a green bay,\nRage, rage against the dying of the light.\n\nWild men who caught and sang the sun in flight,\nAnd learn, too late, they grieved it on its way,\nDo not go gentle into that good night.\n\nGrave men, near death, who see with blinding sight\nBlind eyes could blaze like meteors and be gay,\nRage, rage against the dying of the light.\n\nAnd you, my father, there on the sad height,\nCurse, bless, me now with your fierce tears, I pray.\nDo not go gentle into that good night.\nRage, rage against the dying of the light.")).decode("ascii")},
    ],
    "regex_search": [
        {  # data is 64 random chars, pattern just looks for any digit-nondigit-digit sequence
            "data": "".join(random.choices(string.ascii_letters + string.digits, k=64)),
            "pattern": r"\d\D\d",
        }
        for _ in range(10)
    ],
    "regex_match": [
        {  # data is 64 random chars, pattern just looks for any digit-nondigit-digit sequence
            "data": "".join(random.choices(string.ascii_letters + string.digits, k=64)),
            "pattern": r"\d\D\d",
        }
        for _ in range(10)
    ],
}
# Reset seeds to "truly" random
random.seed()
nprand.seed()


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
        except KeyError:
            log.error("Worker with unknown ID %s attempted to connect", self.worker_id)
            return

        # Send the worker the next item on the queue
        try:
            job_json = w.job_queue.get_nowait()
            self.request.sendall((job_json + "\n").encode(encoding="ascii"))
            log.info("Transmitted work to worker %s", self.worker_id)
            log.debug(job_json)
        except queue.Empty:
            # Worker's queue has been empty since the connection began
            self.request.sendall((SHUTDOWN_PAYLOAD + "\n").encode(encoding="ascii"))
            log.warning(
                "Worker %s requested work while queue empty. Shutdown payload sent.",
                self.worker_id,
            )
            return

        # Now we wait for work to happen and results to come back
        # The socket timeout will limit how long we wait
        try:
            self.data = self.request.recv(12288).strip()
        except socket.timeout:
            log.error("Timed out waiting for worker %s to run %s", self.worker_id, job_json)

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
            log.info("Worker %s's queue is empty. Sending shutdown payload.", self.worker_id)
            self.request.sendall((SHUTDOWN_PAYLOAD + "\n").encode(encoding="ascii"))
        else:
            log.debug("Finished handling worker %s. Sending reboot payload.", self.worker_id)
            self.request.sendall((REBOOT_PAYLOAD + "\n").encode(encoding="ascii"))


        

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
            #log.debug("Added job to worker %s's queue: %s", w.id, json.dumps(cmd))
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
    writer = ThreadsafeCSVWriter(datetime.now().strftime("%Y%m%d.%H%M%S")+".microfaas-log.csv")

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
