import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pandas as pd
from config.log_config import setup_logging
from corelib.database import fetch_data
from corelib.google_sheets import gsheet_writer
import logging.config
from configparser import ConfigParser
from corelib.geocoding import geocode_addresses

# Configuring the logging module of the script
setup_logging()
logger = logging.getLogger(__name__)

# Load configuration
config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.ini'))

# Load secure environment variables
with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'secure.env')) as f:
    for line in f:
        if line.strip() and not line.startswith('#'):
            key, value = line.strip().split('=', 1)
            os.environ[key] = value


def main():
    """
    Main function to run the project
    :return: Boolean (True or False), corresponding to the results of the project whether it succeeded or not.
    """
    dataframe, status = fetch_data(query=config.get('Database', 'query'),
                                   batch_size=int(config.get('Database', 'batch_size')))

    if dataframe.empty or not status:
        logger.warning("Data extraction from database has failed. Exiting the script.")
        return False
    else:
        logger.info("Data extraction from database has been completed. Initiating geocoding script.")
        dataframe = geocode_addresses(dataframe)

        if 'LONGITUDE' in dataframe.columns and not pd.isnull(dataframe['LONGITUDE']).all():
            gsheet_writer(dataframe)
            return True
        else:
            logger.warning("Longitudes and latitudes data hasn't been extracted, and are not present in the dataframe.")
            gsheet_writer(dataframe)
            return False


if __name__ == "__main__":
    logger.info("Geocoding script has been initiated.")
    success = main()
    if success:
        logger.info("Geocoding script has been completed successfully.")
    else:
        logger.warning("Geocoding script has not been completed successfully.")
