import logging
from logging.handlers import RotatingFileHandler

LOG_MAX_BYTES = 20 * 1024 * 1024  # 5 MB
LOG_BACKUP_COUNT = 3  # Keep 3 rotated log files

def get_logger(config):
    logger = logging.getLogger("worker")
    if not logger.handlers:  # Avoid adding multiple handlers if called again
        WORKER_NAME = config.get("worker_name", "Unknown")
        LOG_FILE = config["log_file"]
        file_handler = RotatingFileHandler(LOG_FILE, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
        format_string = f"[{WORKER_NAME}] [%(asctime)s] %(levelname)s: %(message)s"
        file_handler.setFormatter(logging.Formatter(format_string))
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(format_string))
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)
        logger.setLevel(logging.INFO)
    return logger