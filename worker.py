#!/sbin/micropython
import workloads
try:
    import usocket as socket
except:
    import socket
try:
    import ujson as json
except:
    import json
try:
    import utime as time
except:
    import time

def shutdown():
    return workloads.shutdown(0)

# All timing values in nanoseconds
timing = {
    'init': time.time_ns(),
    'begin_exec': None,
    'end_exec': None,
    'pre_reply': None,
    'rel_duration': time.ticks_us() # This will be converted to ns below
}

s = socket.socket()
ai = socket.getaddrinfo("192.168.1.1", 63302)
addr = ai[0][-1]
s.connect(addr)

# Send a few garbage bytes as our ID, forcing orchestrator
# to use our IP as an ID
s.write(b"pl\n")

# Receive JSON-packed command from orchestrator
cmd_json = s.readline()
print("DEBUG: Orchestrator offers: " + str(cmd_json))
try:
    cmd = json.loads(cmd_json)
except ValueError:
    print("ERR: Orchestrator sent malformed JSON!")
    s.close()
    shutdown()

# Try to execute the requested function
timing['begin_exec'] = time.time_ns()
try:
    result = workloads.FUNCTIONS[cmd['f_id']](cmd['f_args'])
except KeyError:
    print("ERR: Bad function ID or malformed array")
    s.close()
    shutdown()
timing['end_exec'] = time.time_ns()

# Construct the reply to the orchestrator
reply = {
    'f_id': cmd['f_id'],
    'i_id': cmd['i_id'],
    'result': result,
    'timing': timing
}

reply['timing']['pre_reply'] = time.time_ns()
# Calculate duration just to ensure NTP didn't mess with the clock before execution
reply['timing']['rel_duration'] = time.ticks_diff(time.ticks_us(), timing['rel_duration'])*1000

# Send the result back to the orchestrator
s.write(json.dumps(reply) + "\n")

# Close the socket
s.close()

print("INFO: Work complete. Shutting down...")

# Immediate Shutdown
shutdown()
