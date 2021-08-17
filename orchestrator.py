#!/usr/bin/env python3
import argparse
import json
import logging as log
import queue
import random
import socket
import socketserver
import string
import threading
import time
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError

from numpy import random as nprand

import settings as s
from recording import ThreadsafeCSVWriter
from workers import BBBWorker, VMWorker
from commands import COMMANDS

# Log Level
log.basicConfig(level=s.LOG_LEVEL)

# Check command line argument for VM flag
parser = argparse.ArgumentParser()
parser.add_argument('--vm', action='store_true', help="Only use VMWorkers")
parser.add_argument('--bbb', action='store_true', help="Only use BBBWorkers")
parser.add_argument('--ids', action="store", help="Only use workers with specified IDs in comma-separated list (may be further constrained by --vm or --bbb)")
ARGS = parser.parse_args()

# Generate worker set
WORKERS = {}
for id, worker_tuple in s.AVAILABLE_WORKERS.items():
    if worker_tuple[0] == "BBBWorker":
        WORKERS[id] = BBBWorker(int(id), worker_tuple[1])
    elif worker_tuple[0] == "VMWorker":
        WORKERS[id] = VMWorker(int(id), worker_tuple[1])
    else:
        log.error("Bad worker specification: %s %s", id, worker_tuple)

POSTFIX = ""
if ARGS.vm and ARGS.bbb:
    raise argparse.ArgumentError("Cannot combine options --vm and --bbb")
elif ARGS.vm:
    log.info("User requested we use VMWorkers only")
    WORKERS = {k:v for k,v in WORKERS.items() if isinstance(v, VMWorker)}
    POSTFIX = "-vm"
elif ARGS.bbb:
    log.info("User requested we use BBBWorkers only")
    WORKERS = {k:v for k,v in WORKERS.items() if isinstance(v, BBBWorker)}
    POSTFIX = "-bbb"

if ARGS.ids is not None:
    requested_ids = [int(x.strip()) for x in ARGS.ids.split(",")]
    WORKERS = {k:v for k,v in WORKERS.items() if v.id in requested_ids}

log.info("Using the following workers: %s", str(WORKERS.values()))

START_TIME = datetime.now()


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Set the timeout for blocking socket operations
        self.request.settimeout(s.SOCK_TIMEOUT)

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
            log.info("Transmitted work to %s", w)
            log.debug(job_json)
        except queue.Empty:
            # Worker's queue has been empty since the connection began
            try:
                try:
                    self.request.sendall(w.power_down_payload())
                except NotImplementedError:
                    # Worker doesn't support shutting itself down, so send the command out-of-band
                    w._power_down_externally()
                log.warning("%s requested work while queue empty. Power-off command sent.", w)
            except Exception as ex:
                log.warning("%s requested work while queue empty, but unable to power-off: %s", w, ex)
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
            try:
                try:
                    self.request.sendall(w.power_down_payload())
                except NotImplementedError:
                    # Worker doesn't support shutting itself down, so send the command out-of-band
                    w._power_down_externally()
                log.info("%s's queue is empty. Sent power-down command.", w)
            except Exception as ex:
                log.warning("%s's queue is empty, but unable to power-down: %s", w, ex)
        else:
            self.request.sendall(w.reboot_payload())
            log.debug("Finished handling %s. Sent reboot command.", w)



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
            
            try:
                f_id = random.choice(list(COMMANDS.keys()))
            except IndexError as e:
                log.debug("COMMANDS is empty, continuing...")
                continue

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
                try:
                    w.power_up_async()
                    log.info("%s has work to do, requested async power-up", w)
                except Exception as ex:
                    log.debug("%s has work to do, but unable to power-up: %s", w, ex)
                except RuntimeError as ex:
                    log.error("Potential race condition/deadlock: %s", ex)
            count -= 1
        time.sleep(s.LOAD_GEN_PERIOD)
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
                try:
                    w.power_up_async(block_if_locked=False)
                    log.warning("Haven't heard from %s since %s, requested power-up", w, w.last_connection)
                except Exception as ex:
                    log.warning("Haven't heard from %s since %s, but unable to power-up: %s", w, w.last_connection, ex)
                except RuntimeError as ex:
                    log.error("Haven't heard from %s since %s, but unable to power-up: %s", w, w.last_connection, ex)

            # Check queues again
            all_queues_not_empty = all_queues_not_empty or not w.job_queue.empty()
        time.sleep(5)
    log.info("Health monitor exiting (all queues empty)")

if __name__ == "__main__":

    # Set up CSV writer
    writer = ThreadsafeCSVWriter("microfaas{}.results.{}.csv".format(POSTFIX, datetime.now().strftime("%Y-%m-%d.%I%M%S%p"),))

    # Set up load generation thread
    load_gen_thread = threading.Thread(
        target=load_generator, daemon=False, args=(s.FUNC_EXEC_COUNT,)
    )
    load_gen_thread.start()

    # Set up health monitor thread
    health_monitor_thread = threading.Thread(
        target=health_monitor, daemon=True
    )
    health_monitor_thread.start()

    # Set up server thread
    server = ThreadedTCPServer((s.HOST, s.PORT), ThreadedTCPRequestHandler)
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
