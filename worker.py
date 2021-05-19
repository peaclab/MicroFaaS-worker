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
    return workloads.fwrite({'path': "/proc/sysrq-trigger", 'data': "o"})

def reboot():
    return workloads.fwrite({'path': "/proc/sysrq-trigger", 'data': "b"})

# All timing values in milliseconds
timing = {
    'init': time.ticks_ms(),
    'begin_exec': None,
    'end_exec': None,
    'fin_counter': None,
    'fin_timestamp': None
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
    reboot()

# Try to execute the requested function
timing['begin_exec'] = time.ticks_ms()
try:
    result = workloads.FUNCTIONS[cmd['f_id']](cmd['f_args'])
except KeyError:
    print("ERR: Bad function ID or malformed array")
    s.close()
    reboot()
timing['end_exec'] = time.ticks_ms()


# fin_counter and fin_timestamp should represent roughly the same moment
timing['fin_counter'] = time.ticks_ms()
# Hopefully NTP has an accurate time for us at this point
timing['fin_timestamp'] = time.time_ns()//100000

# Now we make all times (excl. fin_timestamp) relative to fin_counter
final_timing = {
    'init': time.ticks_diff(timing['init'], timing['fin_counter']),
    'begin_exec': time.ticks_diff(timing['begin_exec'], timing['fin_counter']),
    'end_exec': time.ticks_diff(timing['end_exec'], timing['fin_counter']),
    'fin_timestamp': timing['fin_timestamp']
}

# Construct the reply to the orchestrator
reply = {
    'f_id': cmd['f_id'],
    'i_id': cmd['i_id'],
    'result': result,
    'timing': final_timing
}

# Send the result back to the orchestrator
s.write(json.dumps(reply) + "\n")

# Receive the followup command (usually reboot or shutdown)
cmd_json = s.readline()
print("DEBUG: Orchestrator offers follow-up: " + str(cmd_json))
try:
    cmd = json.loads(cmd_json)
except ValueError:
    print("ERR: Orchestrator sent malformed JSON follow-up!")
    s.close()
    reboot()

# Close the socket
s.close()

# Run the final followup command
workloads.FUNCTIONS[cmd['f_id']](cmd['f_args'])

# Then we'll probably be forcibly rebooted/shutdown

# If we make it here, things are getting weird
print("WARN: Follow-up command allowed execution to continue. Shutting down...")

# Immediate Shutdown
shutdown()
