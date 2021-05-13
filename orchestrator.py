#!/usr/bin/env python3
import socket
import threading
import socketserver
import queue
import time
import random

# List of individual worker IDs
# e.g., if the orchestrator is 192.168.1.1, and workers are 192.168.1.2-11, this should be range(2, 12) 
WORKERS = range(2, 12)

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 1000

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 0.5

# JSON payload to send when we want the worker to power down
SHUTDOWN_PAYLOAD = "shutdown_dummy_str"

class ThreadedTCPRequestHandler(socketserver.StreamRequestHandler):
    def handle(self):
    
        # First check if worker identified itself
        print("DEBUG: Incoming request from " + str(self.client_address[0]))
        try:
            self.worker_id = int(self.rfile.readline().strip())
        except ValueError:
            # It didn't, so try to identify the worker by its IP address
            try:
                self.worker_id = int(self.client_address[0].split('.')[-1])
            except ValueError:
                print("ERR: Could not deduce worker ID for {}. Dropping request.".format(self.client_address[0]))
                return
        
        # Send the worker the next item on the queue
        try:
            self.wfile.write(queues[str(self.worker_id)].get_nowait().encode(encoding="ascii"))
            print("DEBUG: Popped off queue " + str(self.worker_id))
        except queue.Empty:
            # This worker's queue is empty, so tell it to shutdown
            self.wfile.write(SHUTDOWN_PAYLOAD)
            print("WARN: Worker with empty queue requested work")
            return
          
        # Now we wait for work to happen and results to come back
        # TODO: Implement some sort of timeout here (probably using raw sockets)
        
        # self.rfile is a file-like object created by the handler;
        # we can now use e.g. readline() instead of raw recv() calls
        self.data = self.rfile.readline().strip()

        # Print results to console
        print("INFO: {} returned: {}".format(self.worker_id, self.data))
        
        # Worker should now shutdown on its own immediately
        return



class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


def client(ip, port, client_id):
    """
    Dummy client
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        # Connect
        sock.connect((ip, port))
        # Send client ID
        sock.sendall(bytes(str(client_id) + "\n", "ascii"))
        # Receive Job ID
        job_id = str(sock.recv(1024), 'ascii')
        print("CLIENT: Received: {}".format(job_id))
        
        # Pretend to do some work
        time.sleep(1)
        
        # Send a result
        sock.sendall(bytes("dummy_{}_end_{}".format(client_id, random.randint(0,1000)), 'ascii'))


def load_generator(count):
    """
    Load generation thread. Run as daemon
    
    Currently a stub that just gives every queue a function each period
    """
    while count > 0:
        for _, q in queues.items():
            q.put_nowait("DUMMY_QUEUE_ITEM_"+str(random.randint(0,1000)))
            count -= 1
        time.sleep(LOAD_GEN_PERIOD)

if __name__ == "__main__":
    # Host "" means bind to all interfaces
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "", 63302
    
    # Set up queues
    queues = {str(k):queue.Queue() for k in WORKERS}
    
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

        client(ip, port, 6)
        client(ip, port, 4)
        client(ip, port, 5)
        
        # Run server for an hour (TODO: hacky af)
        time.sleep(3600)

        server.shutdown()
