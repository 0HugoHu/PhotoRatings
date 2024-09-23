import os
import logging
import json

# Folder paths
BASE_DIR = os.getcwd()
RAW_FOLDER = os.path.join(BASE_DIR, 'images_raw')
UNRATED_FOLDER = os.path.join(BASE_DIR, 'images_unrated')
RATED_FOLDER = os.path.join(BASE_DIR, 'images_rated')
DEBUG_FOLDER = os.path.join(BASE_DIR, 'images_debug')
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'images_log.json')
OPERATION_LOG_FILE = os.path.join(BASE_DIR, 'logs', 'operations.log')
LOG_ARCHIVE_FOLDER = os.path.join(BASE_DIR, 'logs_archive')

# Constants
PARTITION_SIZE = 100  # Number of images per partition
SCHEDULER_INTERVAL_MOVE_TO_UNRATED = 60 * 60  # Time interval (seconds)
SCHEDULER_INTERVAL_CHECK_LOG_SIZE = 60 * 30 # Time interval (seconds)
MAX_FILE_SIZE_BYTES = 2 * 1024 * 1024  # MB
MAX_LOG_FILE_SIZE_BYTES = 20 * 1024 * 1024  # MB

# Configure logging
logging.basicConfig(
    filename=OPERATION_LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def log_operation(message):
    """Log an operation message."""
    logging.info(message)


if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w') as log_file:
        json.dump({}, log_file)


if not os.path.exists(LOG_ARCHIVE_FOLDER):
    os.makedirs(LOG_ARCHIVE_FOLDER, exist_ok=True)
