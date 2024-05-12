import datetime
import logging
import os
import pickle
from configparser import ConfigParser

import pandas as pd
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from config.log_config import setup_logging

# Configuring the logging module of the script
setup_logging()
logger = logging.getLogger(__name__)

# Load configuration
config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.ini'))

def gsheet_writer(df):
    """
    This function writes the resulted dataframe to a Google Sheet.
    :param df: Dataframe
    :return: None; as it saves the dataframe to the Google Sheet directly.
    """
        # OAuth2.0 Setup
        scopes = os.environ.get('GOOGLE_SCOPE')
        creds = None

        # The file token.pickle stores the user's access and refresh tokens.
        # It is created automatically when the authorization flow completes for the first time.
        pickle_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'token.pickle')
        credential_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'credentials.json')
        if os.path.exists(pickle_path):
            with open(pickle_path, 'rb') as token:
                creds = pickle.load(token)

        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(credential_path, scopes)
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(pickle_path, 'wb') as token:
                pickle.dump(creds, token)

        service = build('sheets', 'v4', credentials=creds, cache_discovery=False)

        # ID of the existing spreadsheet
        spreadsheet_id = config.get('GSheet', 'sheet_id')

        # Add a new sheet to the existing spreadsheet or check if it exist
        sheet_title = f"GeocodingClients-{datetime.datetime.now().strftime('%Y-%m-%d')}"

        # Check if the sheet already exists
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_exists = any(sheet['properties']['title'] == sheet_title for sheet in spreadsheet['sheets'])

        if not sheet_exists:
            # Add a new sheet
            batch_update_spreadsheet_request_body = {
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": sheet_title,
                            }
                        }
                    }
                ]
            }
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                               body=batch_update_spreadsheet_request_body).execute()
        else:
            # Clear the existing sheet
            clear_values_request_body = {}
            service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_title}!A1",
                body=clear_values_request_body
            ).execute()

        # Convert DataFrame to suitable format
        data = df.fillna("").astype(str).values.tolist()
        data.insert(0, df.columns.tolist())

        # Write DataFrame to the sheet
        body = {
            'values': data
        }
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}!A1",
            valueInputOption="RAW",
            body=body
        ).execute()

        logger.info("Added new sheet: {new_sheet_title} in the spreadsheet with ID: {spreadsheet_id}")
