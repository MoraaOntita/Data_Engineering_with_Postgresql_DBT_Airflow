import logging  
import os  
from datetime import datetime  


# Creating a log file name based on the current date and time.
LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"

# Constructing the path where logs will be saved.
logs_path = os.path.join(os.getcwd(),"logs", LOG_FILE)

# Creating the directory for logs if it doesn't exist already.
os.makedirs(logs_path,exist_ok=True)

# Constructing the full path for the log file.
LOG_FILE_PATH = os.path.join(logs_path, LOG_FILE)

# Configuring the logging module with basic settings.
logging.basicConfig(
    filename=LOG_FILE_PATH,  # Setting the filename for the log file.
    format="[%(asctime)s] %(lineno)d %(name)s -%(levelname)s - %(message)s",  # Setting the format of log messages.
    level=logging.INFO,  # Setting the logging level to INFO, which means it will log all INFO level messages and above.
)
