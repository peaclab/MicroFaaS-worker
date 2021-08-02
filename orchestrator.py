#!/usr/bin/env python3
import Adafruit_BBIO.GPIO as GPIO
import argparse
import csv
import json
import logging as log
import os
import queue
import random
import socket
import socketserver
import string
import threading
import time

from binascii import hexlify
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from netcat import Netcat
from numpy import random as nprand
from zlib import compress





# Check command line argument for VM flag
parser = argparse.ArgumentParser()
parser.add_argument('--vm', action='store_true')
VM_MODE = parser.parse_args().vm
if VM_MODE:
    print("VM Mode Activated :D")

# NC Server IP
NC_IP = '127.0.0.1'
NC_PORT = 8888

# TCP Server Setup
# Host "" means bind to all interfaces
# Port 0 means to select an arbitrary unused port
HOST, PORT = "", 63302

# Log Level
log.basicConfig(level=log.INFO)

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 200

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 1

class Worker:
    BTN_PRESS_DELAY = 0.5
    LAST_CONNECTION_TIMEOUT = timedelta(seconds=8)
    POWER_UP_MAX_RETRIES = 6

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

        # Setup GPIO lines
        if not VM_MODE:
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
                if VM_MODE:
                    log.info("Power on VM for worker %s (try #%d)", self.id, retries)
                    #Start vms with nc
                    nc = Netcat(NC_IP, NC_PORT)
                    MAC = "DE:AD:BE:EF:00" + self.pin
                    BOOTARGS = "ip=192.168.1.10" + self.pin + "::192.168.1.1:255.255.255.0:worker" + self.pin + ":eth0:off:1.1.1.1:8.8.8.8:209.50.63.74 " + "root=/dev/ram0 rootfstype=ramfs rdinit=/sbin/init console=ttyS0"
                    KVM_COMMAND = "kvm -M microvm -vga none -nodefaults -no-user-config -nographic -kernel ~/bzImage  -append \"" + BOOTARGS + "\" -netdev tap,id=net0,script=test/ifup.sh,downscript=test/ifdown.sh    -device virtio-net-device,netdev=net0,mac=" + MAC
                    log.debug("Sending nc command: " + KVM_COMMAND)
                    nc.write(KVM_COMMAND.encode())
                    nc.close()

                else:
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
    def __init__(self, metric_path="microfaas-log.csv", result_path="microfaas-results.csv") -> None:
        self._file_lock = threading.Lock()
        self._metric_file_handle = open(metric_path, "a+t")
        self._result_file_handle = open(result_path, "a+t")
        self._metric_writer = csv.DictWriter(
            self._metric_file_handle,
            fieldnames=[
                "invocation_id",
                "worker",
                "function_id",
                "exec_time",
                "rtt",
                "timestamp"
            ],
        )
        self._result_writer = csv.DictWriter(
            self._result_file_handle,
            fieldnames=[
                "invocation_id",
                "result",
            ],
        )

        with self._file_lock:
            self._metric_writer.writeheader()
            self._metric_file_handle.flush()
            os.fsync(self._metric_file_handle)

            self._result_writer.writeheader()
            self._result_file_handle.flush()
            os.fsync(self._result_file_handle)

    def __del__(self):
        with self._file_lock:
            self._metric_file_handle.flush()
            os.fsync(self._metric_file_handle)
            self._metric_file_handle.close()

            self._result_file_handle.flush()
            os.fsync(self._result_file_handle)
            self._result_file_handle.close()

    def save_raw_result(self, worker_id, data_json, rtt, timestamp):
        """
        Process and save a raw JSON string recv'd from a worker to CSV
        """

        # data_json should look like {i_id, f_id, result, exec_time}
        # where exec_time is in milliseconds
        try:
            data = json.loads(data_json)
        except JSONDecodeError as e:
            log.error("Cannot save malformed JSON from worker %s: %s", worker_id, e)
            return False

        # Convert relative timestamps to absolutes, and milliseconds to fractional seconds
        try:
            metric_row = {
                "invocation_id": data["i_id"],
                "worker": worker_id,
                "function_id": data["f_id"],
                "exec_time": int(data["exec_time"]),
                "rtt": int(rtt),
                "timestamp": timestamp
            }

            result_row = {
                "invocation_id": data["i_id"],
                "result": data['result']
            }
        except KeyError as e:
            log.error("Bad schema: %s", e)
            return False
        except ValueError as e:
            log.error("Bad cast: %s", e)
            return False

        with self._file_lock:
            self._metric_writer.writerow(metric_row)
            self._metric_file_handle.flush()
            os.fsync(self._metric_file_handle)

            self._result_writer.writerow(result_row)
            self._result_file_handle.flush()
            os.fsync(self._result_file_handle)

        return metric_row['invocation_id']

# Mapping of worker IDs to GPIO lines
# We assume the ID# also maps to the last octet of the worker's IP
# e.g., if the orchestrator is 192.168.1.2, and workers are 192.168.1.3-12, this should be range(3, 13)
if VM_MODE:
    WORKERS = {
        "3": Worker(3, ":03"),
        "4": Worker(4, ":04"),
        "5": Worker(5, ":05"),
    }
else:
    WORKERS = {
        "3": Worker(3, "P9_15"),
        "4": Worker(4, "P9_23"),
        "5": Worker(5, "P9_25"),
        # "6": Worker(6, "P9_27"),
        # "7": Worker(7, "P8_8"),
        # "8": Worker(8, "P8_10"),
        # "9": Worker(9, "P8_12"),
        # "10": Worker(10, "P8_14"),
        # "11": Worker(11, "P8_26"),
        # "12": Worker(12, "P9_12"),
    }

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
SOCK_TIMEOUT = 90
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
    "redis_modify": [
    	  {
    	      "id": "".join(random.choice(["Jenny", "Jack", "Joe"])),
    	      "spend": "".join(random.choices(string.digits, k=3))
    	  }
    	  for _ in range(10)
    ],
    "redis_insert": [
    	  {
    	      "id": "".join(random.choices(string.digits, k=10)),
    	      "balance": "".join(random.choices(string.digits, k=3))
    	  }
    	  for _ in range(10)
    ],
    "psql_inventory": [
        # this workload doesn't actually need input, but we need something here
        # so the load generator will schedule it
        {"a": 0},
        {"a": 1},
        {"a": 2},
        {"a": 4},
    ],
    "psql_purchase": [
        {  # id is a rand int upto 60
            "id": random.randint(1, 60)
        }
        for _ in range(10)
    ],
    "upload_file": [
        # we upload files that already exist in workers' initramfs (specifically in /etc)
        # in order to avoid adding or generating dummy files at runtime 
        {"file": "group"},
        {"file": "hostname"},
        {"file": "hosts"},
        {"file": "inittab"},
        {"file": "passwd"},
        {"file": "profile"},
        {"file": "resolv.conf"},
        {"file": "shadow"},
    ],
    "download_file": [
        # we assume these files already exist in the MinIO filestore 
        {"file": "file-sample_1MB.doc"},
        {"file": "file_example_ODS_5000.ods"},
        {"file": "file_example_PPT_1MB.ppt"},
    ],
    "redis_modify": [
        {
            "id": "".join(random.choice(["Jenny", "Jack", "Joe"])),
            "spend": str(random.randint(0,999))
    	}
        for _ in range(10)
    ],
    "redis_insert": [
        {
            "id": str(random.randint(1000000,9999999)),
            "balance": str(random.randint(0,999))
    	}
    	for _ in range(10)
    ],
    "upload_kafka": [
        {
            "groupID": 2,
            "consumerID" : "br1-780e17d8-549d-4531-ac95-c29afa751d5e",
            "topic" : "SampleTopic",
            "message" : "Hello World ".join(random.choices(string.digits, k=10))
        }
        for _ in range(10)
    ],
    "read_kafka": [
        {
            "groupID": 2,
            "consumerID" : "br1-780e17d8-549d-4531-ac95-c29afa751d5e"
        }
    ]
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
            send_time = time.monotonic() * 1000
            self.request.sendall((job_json + "\n").encode(encoding="ascii"))
            log.info("Transmitted work to worker %s", self.worker_id)
            log.debug(job_json)
        except queue.Empty:
            # Worker's queue has been empty since the connection began
            if VM_MODE:
                nc = Netcat(NC_IP, NC_PORT)
                nc.write(("pkill -of \"" + w.pin + "\"\n").encode())
                nc.close()
            else:
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
            recv_time = time.monotonic() * 1000
        except socket.timeout:
            log.error("Timed out waiting for worker %s to run %s", self.worker_id, job_json)
            return

        # Calculate Round Trip Time
        rtt = recv_time - send_time

        # Save results to CSV
        log.debug("Worker %s returned: %s", self.worker_id, self.data)
        writer_result = writer.save_raw_result(self.worker_id, self.data, rtt, time.strftime("%Y-%m-%d %H:%M:%S"))
        if not writer_result:
            log.error("Failed to process results from worker %s!", self.worker_id)
        else:
            log.info("Processed results of invocation %s from worker %s", writer_result, self.worker_id)

        # Check if there's more work for it in its queue
        # If yes, send reboot. Otherwise send shutdown
        if w.job_queue.empty():
            log.info("Worker %s's queue is empty. Sending shutdown payload.", self.worker_id)
            if VM_MODE:
                nc = Netcat(NC_IP, NC_PORT)
                nc.write(("pkill -of \"" + w.pin + "\"\n").encode())
                nc.close()
            else:
                self.request.sendall((SHUTDOWN_PAYLOAD + "\n").encode(encoding="ascii"))
        else:
            log.debug("Finished handling worker %s. Sending reboot payload.", self.worker_id)
            self.request.sendall((REBOOT_PAYLOAD + "\n").encode(encoding="ascii"))


        

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def server_bind(self) -> None:
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        log.info("Server thread bound to %s", self.server_address)
        return


def load_generator(count):
    """
    Load generation thread. Run as daemon

    Every period, picks a random number of workers and puts a new job on their queue
    """
    log.info("Load generator started (limit: %d total invocations)", count)
    # Ensure jobs are run in a balanced way
    job_counts = dict({k:(count // len(COMMANDS)) for k, _ in COMMANDS.items()})
    while count > 0:
        for _, w in random.sample(WORKERS.items(), random.randint(1,len(WORKERS))):
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

            # Keep track of how many times we've run this job
            job_counts[f_id] -= 1
            if job_counts[f_id] <= 0:
                # If we've run it enough, remove it from the list of commands
                log.info("Enough invocations of %s have been queued up, so popping from COMMANDS", f_id)
                COMMANDS.pop(f_id, None)

            if q_was_empty:
                # This worker's queue was empty, meaning it probably isn't
                # powered on right now. Now that it has work, power it up
                # asynchronously (so that this thread can continue)
                log.debug("Requesting async power-up of worker %s", w.id)
                try:
                    w.power_up_async()
                except RuntimeError as e:
                    log.error("Potential race condition/deadlock: %s", e)
            count -= 1
        time.sleep(LOAD_GEN_PERIOD)
    log.info("Load generator exiting (queuing complete)")

def health_monitor(timeout=120):
    timeout_delta = timedelta(seconds=timeout)
    all_queues_not_empty = True
    while all_queues_not_empty:
        all_queues_not_empty = False
        for _, w in WORKERS.items():
            if (not w.job_queue.empty() 
                  and w.last_connection != datetime.min
                  and datetime.now() - w.last_connection > timeout_delta):
                log.warning("Haven't heard from worker %s since %s, requesting power-up", w.id, w.last_connection)
                try:
                    w.power_up_async(block_if_locked=False)
                except RuntimeError as e:
                    log.warning("Power-up request denied: %s", e)

            # Check queues again
            all_queues_not_empty = all_queues_not_empty or not w.job_queue.empty()
        time.sleep(5)
    log.info("Health monitor exiting (all queues empty)")

if __name__ == "__main__":

    # Set up CSV writer
    writer = ThreadsafeCSVWriter(datetime.now().strftime("%Y%m%d.%H%M%S")+".microfaas-log.csv")

    # Set up load generation thread
    load_gen_thread = threading.Thread(
        target=load_generator, daemon=False, args=(FUNC_EXEC_COUNT,)
    )
    load_gen_thread.start()

    # Set up health monitor thread
    health_monitor_thread = threading.Thread(
        target=health_monitor, daemon=True
    )
    health_monitor_thread.start()

    # Set up server thread
    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
    with server:
        ip, port = server.server_address

        # Start a thread with the server -- that thread will then start one
        # more thread for each request
        server_thread = threading.Thread(target=server.serve_forever)
        # Run the server thread when the main thread terminates
        server_thread.daemon = False
        server_thread.start()
        #print("Server loop running in thread:", server_thread.name)

        # Run at least until we finish queuing up our workloads
        load_gen_thread.join()

        # Then check if it's been a while since our last request
        all_queues_not_empty = True
        while all_queues_not_empty:
            all_queues_not_empty = False
            for _, w in WORKERS.items():
                all_queues_not_empty = all_queues_not_empty or not w.job_queue.empty()
            time.sleep(2)

        server.shutdown()
