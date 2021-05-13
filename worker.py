#!/sbin/micropython
try:
    import usocket as socket
except:
    import socket

s = socket.socket()
ai = socket.getaddrinfo("192.168.1.1", 63302)
addr = ai[0][-1]
s.connect(addr)

#s.send(b"GET / HTTP/1.0\r\n\r\n")
with open("/tmp/boot-connect.log", 'w') as f:
    # Receive function ID from orchestrator
    f.write("FID received: ")
    f.write(s.recv(1024))

    # TODO: Execute the function

    # Send the result back to the orchestrator
    s.send(b"dummy_bbb_result")

    # Record uptime (TODO: replace with real timestamping)
    with open("/proc/uptime", 'r') as u:
        f.write("\n\n")
        f.write(u.read())

# Close the socket
s.close()