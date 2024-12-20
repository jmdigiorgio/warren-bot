import logging
from pythonjsonlogger import jsonlogger

# Configure root logger
logger = logging.getLogger()

# Remove any existing handlers
for handler in logger.handlers[:]:
    logger.removeHandler(handler)

# Add our JSON handler
logHandler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Function to get logger for a specific module
def get_logger(name):
    return logging.getLogger(name) 