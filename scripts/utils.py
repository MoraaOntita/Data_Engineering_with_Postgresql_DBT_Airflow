# utils.py
import logging

# Define logger
logger = logging.getLogger(__name__)

# Function for logging errors
def log_error(error_message: str):
    logger.error(error_message)

