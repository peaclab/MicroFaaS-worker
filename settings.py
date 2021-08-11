# NC Server IP
NC_IP = '192.168.1.201'
NC_PORT = 1152

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

# How many seconds to "hold down the power button" when powering-up BeagleBones 
BTN_PRESS_DELAY = 0.5

# How long (in seconds) to wait for a worker to make a post-boot connection before retrying
LAST_CONNECTION_TIMEOUT = 10

# How many times to retry a power-up request (see LAST_CONNECTION_TIMEOUT)
POWER_UP_MAX_RETRIES = 6