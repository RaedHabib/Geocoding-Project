import logging
import os
from configparser import ConfigParser

import mysql.connector
import pandas as pd

from config.log_config import setup_logging

# Configuring the logging module of the script
setup_logging()
logger = logging.getLogger(__name__)

# Load configuration
config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.ini'))

with open(os.path.join(os.path.dirname(__file__), '..', 'config', 'secure.env')) as f:
    for line in f:
        if line.strip() and not line.startswith('#'):
            key, value = line.strip().split('=', 1)
            os.environ[key] = value

# Database connection parameters
db_config = {
    'host': config.get('Database', 'host'),
    'port': config.get('Database', 'port'),
    'user': os.environ.get('DATABASE_USER'),
    'passwd': os.environ.get('DATABASE_PASSWORD'),
    'database': config.get('Database', 'name')
}

# Batch information
batch_fetch = config.get('Database', 'batch_fetch').lower() == 'true'
batch_size = int(config.get('Database', 'batch_size'))

# SQL query
query = config.get('Database', 'query')


def fetch_data(query=query, batch_size=batch_size):
    """
    This function fetches data from the database, whether at specified batches or all at once.
    :param query: The query to execute.
    :param batch_size: The number of records to fetch at one time. If batch is set to False, it will ignore and fetch all at once.
    :return: Dataframe containing the fetched data.
    """
    logger.info("Establishing connection with the database.")
    try:
        if batch_fetch:
            logger.info("Batch operation enabled.")
            while True:
                with mysql.connector.connect(**db_config) as conn:
                    with conn.cursor(buffered=True) as cursor:
                        offset = 0
                        batch_dfs = []
                        batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                        cursor.execute(batch_query)
                        results = cursor.fetchall()
                        if not results:
                            break
                        df = pd.DataFrame(results, columns=[col[0] for col in cursor.description])
                        batch_dfs.append(df)
                        offset += batch_size

                full_df = pd.concat(batch_dfs, ignore_index=True)

                return full_df, True

        else:
            try:
                conn = mysql.connector.connect(**db_config)
                cursor = conn.cursor(buffered=True)

                offset = 0
                logger.info("Starting to fetch complete data.")
                cursor.execute(query)
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=[col[0] for col in cursor.description])
                logger.info("Data fetch complete.")

                return df, True

            except mysql.connector.Error as err:
                logger.error(f"An error occurred: {err}")
                return None, False

    except mysql.connector.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        return pd.DataFrame(), False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return pd.DataFrame(), False

    finally:
        if 'cursor' in locals():
            cursor.close()
            logger.info("Cursor has been disconnected successfully.")
        if 'conn' in locals() and conn.is_connected():
            conn.close()
            logger.info("Connection with the database has been closed.")
