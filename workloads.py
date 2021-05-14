import math
import sys
try:
    import utime as time
except:
    import time
try:
    import uhashlib as hashlib
except:
    import hashlib
try:
    import ubinascii as binascii
except:
    import binascii

# https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/float_operation/lambda_function.py
def float_operations(params):
    n = params['n']
    start = time.time()
    for i in range(0, n):
        sin_i = math.sin(i)
        cos_i = math.cos(i)
        sqrt_i = math.sqrt(i)
    latency = time.time() - start
    return latency

def cascading_sha256(params):
    data = params['data']
    rounds = params['rounds']
    sha256_data = hashlib.sha256(data).digest()
    for _ in range(rounds-1):
        sha256_data = hashlib.sha256(sha256_data).digest()
    return binascii.hexlify(sha256_data)

def cascading_md5(params):
    data = params['data']
    rounds = params['rounds']
    md5_data = hashlib.md5(data).digest()
    for _ in range(rounds-1):
        md5_data = hashlib.md5(md5_data).digest()
    return binascii.hexlify(md5_data)

def shutdown(params):
    try:
        with open("/proc/sysrq-trigger", 'w') as f:
            # write 'o' to halt, 'b' to reboot
            f.write('o')
    except EnvironmentError:
        print("ERR: Shutdown request failed. Are sysrq's enabled?")
    finally:
        # We shouldn't actually make it this far
        sys.exit()
    return

# Dictionary mapping available function names to their IDs
FUNCTIONS = {"float_operations": float_operations,
    "cascading_sha256": cascading_sha256,
    "cascading_md5": cascading_md5,
    "shutdown": shutdown}