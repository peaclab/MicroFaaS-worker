import math
import sys
import random
import micropg as p
import json
import ubinascii

try:
    from ulab import numpy as np
    from ulab import scipy as spy
except:
    import numpy as np
    import scipy as spy
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
try:
    import uzlib as zlib
except:
    import zlib
try:
    import ure as re
except:
    import re
import ucryptolib

try:
    import picoredis as pr
except:
    import redis as pr

try:
    import urequests as urq
except:
    import requests as urq
try:
    import uio as io
except:
    import io


# https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/float_operation/lambda_function.py
def float_operations(params):
    n = params["n"]
    start = time.time()
    for i in range(0, n):
        sin_i = math.sin(i)
        cos_i = math.cos(i)
        sqrt_i = math.sqrt(i)
    latency = time.time() - start
    return latency


# https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/matmul/lambda_function.py
def matmul(params):
    # ulab doesn't have np.random, so we have the orch. generate A & B for us
    A = np.array(params["A"], dtype=np.float)
    B = np.array(params["B"], dtype=np.float)

    start = time.time()
    C = np.dot(A, B)  # Replaced np.matmul(A, B)
    latency = time.time() - start
    return latency


# https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/linpack/lambda_function.py
def linpack(params):
    A = np.array(params["A"], dtype=np.float)
    B = np.array(params["B"], dtype=np.float)
    n = A.shape()[0]

    # LINPACK benchmarks
    ops = (2.0 * n) * n * n / 3.0 + (2.0 * n) * n

    # # Create AxA array of random numbers -0.5 to 0.5
    # A = random.random_sample((n, n)) - 0.5
    # B = A.sum(axis=1)

    # # Convert to matrices
    # A = matrix(A)
    # B = matrix(B.reshape((n, 1)))

    # Ax = B
    start = time.time()
    x = spy.linalg.solve_triangular(A, B)  # Replaced np.linalg.solve()
    latency = time.time() - start

    mflops = ops * 1e-6 / latency

    return latency


# Inspired by https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/chameleon/lambda_function.py
# Adapted from https://www.csestack.org/python-generate-html/
def html_generation(params):
    n = int(params["n"])

    output = "<html><table><tr><th>Char</th><th>ASCII</th></tr>"
    for _ in range(n):
        ascii_code = random.getrandbits(8)
        output += "<tr><td>" + str(chr(ascii_code)) + "</td><td>" + str(ascii_code) + "</td></tr>"

    output += "</table></html>"

    return output


# https://github.com/kmu-bigdata/serverless-faas-workbench/blob/master/aws/cpu-memory/pyaes/lambda_function.py
def pyaes(params):
    MODE_ECB = 1
    message = params["data"]  # Data length must be a multiple of 16
    num_of_iterations = params["rounds"]

    # 128-bit key (16 bytes)
    KEY = b"\xa1\xf6%\x8c\x87}_\xcd\x89dHE8\xbf\xc9,"

    start = time.time()

    # Encryption
    ciphertext = message
    for _ in range(num_of_iterations):
        aes = ucryptolib.aes(KEY, MODE_ECB)
        ciphertext = aes.encrypt(ciphertext)

    # Decryption
    plaintext = ciphertext
    for _ in range(num_of_iterations):
        aes = ucryptolib.aes(KEY, MODE_ECB)
        plaintext = aes.decrypt(plaintext)
    latency = time.time() - start

    return {"latency": latency, "data": plaintext}


def cascading_sha256(params):
    data = params["data"]
    rounds = params["rounds"]
    sha256_data = hashlib.sha256(data).digest()
    for _ in range(rounds - 1):
        sha256_data = hashlib.sha256(sha256_data).digest()
    return binascii.hexlify(sha256_data)


def cascading_md5(params):
    data = params["data"]
    rounds = params["rounds"]
    md5_data = hashlib.md5(data).digest()
    for _ in range(rounds - 1):
        md5_data = hashlib.md5(md5_data).digest()
    return binascii.hexlify(md5_data)


def zlib_decompress(params):
    data = binascii.unhexlify(params["data"])
    return binascii.hexlify(zlib.decompress(data))


def regex_search(params):
    data = params["data"]
    pattern = params["pattern"]

    try:
        return re.search(pattern, data).group(0)
    except:
        return 0


def regex_match(params):
    data = params["data"]
    pattern = params["pattern"]

    try:
        return re.match(pattern, data).group(0) is not None
    except:
        return False


def fwrite(params):
    try:
        with open(params["path"], "w") as f:
            f.write(params["data"])
    except EnvironmentError:
        print("ERR: Write request failed. Are sysrq's enabled?")
    return


def redis_modify(params):
    try:
        # Connect to database
        r = pr.Redis(host="192.168.1.157")
        r.auth("microfaas")

        # Modify data: deduct spendage from balance
        r.incrby(params["id"], -1 * int(params["spend"]))

        # Return new balance
        new_balance = r.get(params["id"])
        # Close database connection
        r.close()
        return new_balance
    except:
        return False


def redis_insert(params):

    try:
        # Connect to database
        r = pr.Redis(host="192.168.1.157")
        r.auth("microfaas")

        # Insert data: inserts new id and its balance
        r.set(params["id"], int(params["balance"]))

        # Return balance of new user
        bal = r.get(params["id"])
        # Close connection
        r.close()
        return bal
    except:
        return False


def psql_inventory(params):
    try:
        # Establish connection to the postgreSQL database called bostonautosales
        conn = p.connect(
            user="postgres",
            host="192.168.1.156",
            port="5432",
            database="bostonautosales",
        )

        # Create cursor to read parameters from inventory table in ascending order by Car_Model_Year
        cur = conn.cursor()
        cur.execute(
            "select Car_Make, Car_Model, Car_Model_Year, Number_in_Stock, id from inventory order by id"
        )

        allCars = cur.fetchall()
        retStr = ""
        carCount = 0
        # Loop through all the cars that were fetched and create a string that shows all the cars in stock
        for index, car in enumerate(allCars):
            currCar = car[0] + " " + car[1] + " " + car[2]
            retStr += str(car[4]) + ") " + currCar + ": " + str(car[3]) + " in stock    "
            carCount += car[3]
        retStr += "Total cars in stock: " + str(carCount)
        cur.close()
        return retStr

    except:
        return False


def psql_purchase(params):
    try:
        # Establish connection to the postgreSQL database called bostonautosales
        conn = p.connect(
            user="postgres",
            password="postgres",
            host="192.168.1.156",
            port="5432",
            database="bostonautosales",
        )

        # Create cursor for database
        cur = conn.cursor()

        # Fetch the current number of car in stock using id
        cur.execute("select number_in_stock from inventory where id=" + str(params["id"]))
        numCars = cur.fetchall()[0][0]
        # Decrement the number of cars using id
        cur.execute(
            "update inventory set Number_in_Stock = "
            + str(numCars - 1)
            + "where id = "
            + str(params["id"])
        )
        # Commit new number_in_stock to the database
        conn.commit()

        cur.close()
        return True

    except:
        return False


def upload_file(params):
    try:
        url = "http://192.168.1.158:9000/bucket/" + params["file"]
        local_path = "/etc/" + params["file"]
        try:
            with io.open(local_path, "rb") as f:
                fi = f.read()
        except:
            print("Can't open file.")
            return False

        urq.request("PUT", url, data=fi)
        return True
    except:
        return False


def download_file(params):
    try:
        url = "http://192.168.1.158:9000/bucket/" + params["file"]
        r = urq.get(url)
        local_path = "/root/" + params["file"]
        try:
            with io.open(local_path, "wb") as f:
                f.write(r.content)
        except:
            print("File write error.")
            return False
    except:
        return False

def upload_kafka(params):
    try:
        url='http://192.168.1.166:8081'
        # Post new message to topic
        posting_url = url + '/topics/' + params["topic"]
        body_dict = {
        "records" : [ {
            "value" : params["message"],
            "partition" : 0
        }]
        }
        body_json= json.dumps(body_dict)
        res =urq.post(url = posting_url, headers = {'content-type': 'application/vnd.kafka.json.v2+json','accept': 'application/vnd.kafka.v2+json'},data = body_json)

        # Commit posted message
        commiting_url = url + '/consumers/' + str(params["groupID"]) + '/instances/' + params["consumerID"] + '/offsets'
        body_dict = {
        "offsets" : [ {
            "topic" : params["topic"],
            "partition" : 0,
            "offset" : 0
        } ]
        }
        body_json= json.dumps(body_dict)
        res =urq.post(url = commiting_url, headers = {'content-type': 'application/vnd.kafka.v2+json','accept': 'application/vnd.kafka.v2+json'},data = body_json)
        return True
    except:
        return False


def read_kafka(params):
    try:
        url='http://192.168.1.166:8081'
        # Read committed message
        reading_url = url + '/consumers/' + str(params["groupID"]) + '/instances/' + params["consumerID"] + '/records'
        res =urq.get(url = reading_url, headers = {'accept': 'application/vnd.kafka.binary.v2+json'})
        # Convert response into text and then json. Then, index the value key and convert from Base 64 to byte string to Unicode string
        message = ubinascii.a2b_base64(json.loads(res.text)[0]["value"]).decode("utf-8")
        return message
    except:
        return False




# Dictionary mapping available function names to their IDs
FUNCTIONS = {
    "float_operations": float_operations,
    "cascading_sha256": cascading_sha256,
    "cascading_md5": cascading_md5,
    "matmul": matmul,
    "linpack": linpack,
    "html_generation": html_generation,
    "pyaes": pyaes,
    "zlib_decompress": zlib_decompress,
    "regex_search": regex_search,
    "regex_match": regex_match,
    "redis_modify": redis_modify,
    "redis_insert": redis_insert,
    "psql_inventory": psql_inventory,
    "psql_purchase": psql_purchase,
    "fwrite": fwrite,
    "upload_file": upload_file,
    "download_file": download_file,
    "upload_kafka": upload_kafka,
    "read_kafka": read_kafka
}

