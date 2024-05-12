# Geocoding Project
![image](https://github.com/RaedHabib/Geocoding-Project/assets/127057461/1e0ec2c9-ce88-4b13-aebc-86e700549cf8)

## Description
This project automates the geocoding of addresses using ArcGIS, Google Maps, or Bing Maps and updates a Google spreadsheet in real time. It's designed for seamless data sharing and visualization.

## Features
- **Data Fetching**: Retrieves data dynamically from a specified database.
- **Geocoding Options**: Supports ArcGIS, Google Maps, and Bing Maps for geocoding.
- **Google Sheets Integration**: Updates a Google spreadsheet in real-time for easy sharing and analysis.

## Installation
1. Clone this repository.
2. Check that you have all the necessary Python packages installed in your environment:
   pandas
   mysql-connector-python
configparser
logging
datetime
sys
os
pickle
zipfile
requests
google-api-python-client
google-auth-oauthlib
google-auth
googlemaps
arcgis

4. Configure secure.env with your credentials and API keys.
5. Modify settings.ini to match your database and geocoding preferences.

## Usage
To run the project:
Using bash:
python main/main.py

## Configuration Files
log_config.py: Manages application logging and file rotation.
secure.env: Securely stores sensitive credentials (ensure this file is not tracked by Git).
settings.ini: Contains user-configurable settings like database connection and geocoding service choice.
