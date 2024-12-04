import logging
import os

# Directory for log files (relative path)
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure the directory exists

def create_logger(name="SeaBreezeLogger"):
    """
    Creates a logger that writes information and errors to separate files.
    
    Args:
        name (str): Name of the logger.
    
    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    
    # Avoid adding multiple handlers to the same logger
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)  # Capture all logs from DEBUG level and above

        # File for INFO and DEBUG logs
        info_log_file = os.path.join(LOG_DIR, "info.log")
        info_handler = logging.FileHandler(info_log_file)
        info_handler.setLevel(logging.DEBUG)  # Log DEBUG, INFO, and above
        info_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
        info_handler.setFormatter(info_format)

        # File for ERROR and CRITICAL logs
        error_log_file = os.path.join(LOG_DIR, "error.log")
        error_handler = logging.FileHandler(error_log_file)
        error_handler.setLevel(logging.ERROR)  # Log ERROR and CRITICAL levels only
        error_format = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s - %(message)s')
        error_handler.setFormatter(error_format)

        # Add both handlers to the logger
        logger.addHandler(info_handler)
        logger.addHandler(error_handler)

    return logger
