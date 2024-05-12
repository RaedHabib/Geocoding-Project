import logging
import os
import zipfile
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


# Function to compress log files into ZIP format
def compress_log(file_path):
    zip_file_path = file_path + '.zip'
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.write(file_path, os.path.basename(file_path))
    os.remove(file_path)


def setup_logging():
    # Define the log path
    log_path = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_path, exist_ok=True)

    # Define the path for the current log file
    log_file_path = os.path.join(log_path, 'service.log')

    # Check if the existing log file is from a previous day
    if os.path.exists(log_file_path):
        file_creation_time = os.path.getctime(log_file_path)
        file_creation_date = datetime.fromtimestamp(file_creation_time).date()
        current_date = datetime.now().date()

        if file_creation_date < current_date:
            compress_log(log_file_path)

    # Define a more detailed logging format
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)'

    # Basic logging configuration
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            TimedRotatingFileHandler(
                log_file_path, when="D", interval=1, backupCount=4, encoding='utf-8', delay=True
            ),
            logging.StreamHandler()
        ]
    )