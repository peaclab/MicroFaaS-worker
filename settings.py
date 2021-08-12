from workers import BBBWorker, VMWorker

# NC Server IP
NC_IP = '192.168.1.201'
NC_PORT = 1152

# Socket timeout
SOCK_TIMEOUT = 120

# TCP Server Setup
# Host "" means bind to all interfaces
# Port 0 means to select an arbitrary unused port
HOST, PORT = "", 63302

# How many total functions to run across all workers
FUNC_EXEC_COUNT = 17000

# How often to populate queues (seconds)
LOAD_GEN_PERIOD = 1

# Minimum severity of displayed log messages (DEBUG, INFO, WARNING, ERROR, or CRITICAL)
LOG_LEVEL = "INFO"

# How long (in seconds) to wait after each worker status check to begin another
MONITOR_PERIOD = 0.2

# How many seconds to "hold down the power button" when powering-up BBBWorkers 
BTN_PRESS_DELAY = 0.5

# How long (in seconds) to wait for a worker to make a post-boot connection before retrying
LAST_CONNECTION_TIMEOUT = 10

# How many times to retry a power-up request (see LAST_CONNECTION_TIMEOUT)
POWER_UP_MAX_RETRIES = 6

# How long (in seconds) to wait after script start before issuing power up/down commands
# Useful if you're starting an experiment with pre-powered-up workers
POWER_UP_HOLDOFF_BBB = 10
POWER_UP_HOLDOFF_VM = 0
POWER_DOWN_HOLDOFF_BBB = 10
POWER_DOWN_HOLDOFF_VM = 10

# Worker Setup
# WORKERS maps worker IDs to GPIO lines or MAC-addrs 
# We assume the ID# also maps to the last octet of the worker's IP
# e.g., if the orchestrator is 192.168.1.2, and workers are 192.168.1.3-12, IDs should be range(3, 13)
AVAILABLE_WORKERS = {
    "3": BBBWorker(3, "P9_12"),
    "4": BBBWorker(4, "P9_15"),
    "5": BBBWorker(5, "P9_23"),
    "6": BBBWorker(6, "P9_25"),
    "7": BBBWorker(7, "P9_27"),
    "8": BBBWorker(8, "P8_8"),
    "9": BBBWorker(9, "P8_10"),
    "10": BBBWorker(10, "P8_11"),
    "11": BBBWorker(11, "P8_14"),
    "12": BBBWorker(12, "P9_26"),
    "103": VMWorker(103, ":03"),
    "104": VMWorker(104, ":04"),
    "105": VMWorker(105, ":05"),
}
