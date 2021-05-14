#!/usr/bin/env python3
import socket
import threading
import socketserver
import queue
import time
import random
import string
import json
import Adafruit_BBIO.GPIO as GPIO

# TCP Server Setup
# Host "" means bind to all interfaces
# Port 0 means to select an arbitrary unused port
HOST, PORT = "", 63302

# Mapping of worker IDs to GPIO lines
# We assume the ID# also maps to the last octet of the worker's IP 
# e.g., if the orchestrator is 192.168.1.1, and workers are 192.168.1.2-11, this should be range(2, 12) 
WORKERS = {
    '2': "P9_41",
    '3': "P8_7",
    '4': "P8_8",
    '5': "P8_9",
    '6': "P8_10",
    '7': "P8_11",
    '8': "P8_12",
    '9': "P8_14",
    '10': "P8_15",
    '11': "P8_16"
}

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 1000

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 0.5

# JSON payload to send when we want the worker to power down
SHUTDOWN_PAYLOAD = b"{\"i_id\": \"PWROFF\", \"f_id\": \"shutdown\", \"f_args\": {}}\n"

# Socket timeout
SOCK_TIMEOUT = 60

# Supported workload functions and sample inputs. 
# Make sure COMMANDS.keys() matches your workers' FUNCTIONS.keys()!
random.seed("MicroFaaS", version=2) # Hardcode seed for reproducibility
COMMANDS = {
    "float_operations": [
        {'n': random.randint(1, 1000000)} for _ in range(10)
    ],
    "cascading_sha256": [
        { # data is 64 random chars, rounds is rand int upto 1 mil
            'data': ''.join(random.choices(string.ascii_letters + string.digits, k=64)), 
            'rounds': random.randint(1, 1000000)
        } for _ in range(10)
    ],
    "cascading_md5": [
        { # data is 64 random chars, rounds is rand int upto 1 mil
            'data': ''.join(random.choices(string.ascii_letters + string.digits, k=64)),
            'rounds': random.randint(1, 1000000)
        } for _ in range(10)
    ]
}
random.seed() # Reset seed to "truly" random


def power_up_worker(worker_id):
    """
    Power up a worker by pulsing its PWR_BUT line low for 500ms
    """
    print("DEBUG: Powering up worker " + str(worker_id))
    GPIO.output(WORKERS[str(worker_id)], GPIO.LOW)
    time.sleep(0.5)
    GPIO.output(WORKERS[str(worker_id)], GPIO.HIGH)
    # Do it again, just to be sure
    time.sleep(0.3)
    GPIO.output(WORKERS[str(worker_id)], GPIO.LOW)
    time.sleep(0.5)
    GPIO.output(WORKERS[str(worker_id)], GPIO.HIGH)
    return


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        # Set the timeout for blocking socket operations
        self.request.settimeout(SOCK_TIMEOUT)
    
        # First check if worker identified itself
        print("DEBUG: Incoming request from " + str(self.client_address[0]))
        try:
            # If first few bytes can be casted to an int, assume it's an ID
            self.worker_id = int(self.request.recv(4).strip())
        except ValueError:
            # Otherwise try to identify the worker by its IP address
            try:
                self.worker_id = int(self.client_address[0].split('.')[-1])
            except ValueError:
                print("ERR: Could not deduce worker ID for {}. Dropping request.".format(self.client_address[0]))
                return
        
        # Send the worker the next item on the queue
        try:
            self.request.sendall((queues[str(self.worker_id)].get_nowait() + '\n').encode(encoding="ascii"))
            print("DEBUG: Popped off queue " + str(self.worker_id))
        except queue.Empty:
            # This worker's queue is empty, so tell it to shutdown
            self.request.sendall(SHUTDOWN_PAYLOAD)
            print("WARN: Worker with empty queue requested work")
            return
          
        # Now we wait for work to happen and results to come back
        # The socket timeout will limit how long we wait
        self.data = self.request.recv(8192).strip()

        # Print results to console
        print("INFO: {} returned: {}".format(self.worker_id, self.data))
        
        # Worker should now shutdown on its own immediately
        # Check if there's more work for it in its queue
        if not queues[str(self.worker_id)].empty():
            # Give it a a little time to power down
            time.sleep(1.2)
            # Turn it back on to finish the work
            power_up_worker(self.worker_id)
        return



class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    def server_bind(self) -> None:
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.socket.bind(self.server_address)
        return


# def client(ip, port, client_id):
#     """
#     Dummy client
#     """
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
#         # Connect
#         sock.connect((ip, port))
#         # Send client ID
#         sock.sendall(bytes(str(client_id) + "\n", "ascii"))
#         # Receive Job ID
#         job_id = str(sock.recv(1024), 'ascii')
#         print("CLIENT: Received: {}".format(job_id))
        
#         # Pretend to do some work
#         time.sleep(1)
        
#         # Send a result
#         sock.sendall(bytes("dummy_{}_end_{}".format(client_id, random.randint(0,1000)), 'ascii'))


def load_generator(count):
    """
    Load generation thread. Run as daemon
    
    Currently a stub that just gives every queue a function each period
    """
    while count > 0:
        for w_id, q in queues.items():
            q_was_empty = q.empty()
            f_id = random.choice(list(COMMANDS.keys()))
            cmd = {
                # Invocation ID
                'i_id': ''.join(random.choices(string.ascii_letters + string.digits, k=6)),
                # Function ID (one of COMMANDS.keys())
                'f_id': f_id,
                # Function arguments
                'f_args': random.choice(COMMANDS[f_id]),
            }
            q.put_nowait(json.dumps(cmd))
            #print(json.dumps(cmd))
            if q_was_empty:
                # This worker's queue was empty, meaning it probably isn't
                # powered on right now. Now that it has work, power it up
                power_up_worker(w_id)

            count -= 1
        time.sleep(LOAD_GEN_PERIOD)

if __name__ == "__main__":


    # Set up GPIO lines
    for _, pin in WORKERS.items():
        GPIO.setup(pin, GPIO.OUT)
        GPIO.output(pin, GPIO.HIGH)
    
    # Set up queues
    queues = {str(w_id) : queue.Queue() for w_id, _ in WORKERS.items()}
    
    # Set up load generation thread
    load_gen_thread = threading.Thread(target=load_generator, daemon=True, args=(FUNC_EXEC_COUNT,))
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
