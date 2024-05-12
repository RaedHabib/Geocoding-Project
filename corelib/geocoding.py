geocoding.py
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from configparser import ConfigParser

from arcgis.gis import GIS
from arcgis.geocoding import geocode

import googlemaps
import pandas as pd
import requests
from requests.exceptions import HTTPError

from config.log_config import setup_logging

# Configuring the logging module of the script
setup_logging()
logger = logging.getLogger(__name__)

# Load configuration
config = ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '..', 'config', 'settings.ini'))

# Function to geocode address using Google Maps
def geocode_google(address, gmaps):
    """
    This function simply applies Google Maps API to geocode address and returns corresponding Latitude and Longitude.
    :param address: Full address details including city, state, zip code and country code.
    :param gmaps: Instantiated Google Maps API, you have to register and get your API key first.
    :return: Long. and lat. for the given address
    """
    try:
        geocode_result = gmaps.geocode(address)
        if geocode_result:
            latitude = geocode_result[0]['geometry']['location']['lat']
            longitude = geocode_result[0]['geometry']['location']['lng']
            return longitude, latitude
        else:
            logger.warning(f"No geocoding result for address: {address}")
            return None, None
    except Exception as e:
        logger.error(f"Error occurred during geocoding: {e}")
        return None, None

# Function to geocode address using Bing Maps
def geocode_bing(address, bing_maps_key):
    """
    This function simply applies Bing Maps API to geocode address and returns corresponding Latitude and Longitude.
    :param address: Full address details including city, state, zip code and country code.
    :param bing_maps_key: Bing Maps API key, you have to register and get your API key first.
    :return: Lat. and long. for the given address
    """
    base_url = "http://dev.virtualearth.net/REST/v1/Locations"
    params = {
        "query": address,
        "key": bing_maps_key
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        if data['resourceSets']:
            resources = data['resourceSets'][0]['resources']
            if resources:
                coordinates = resources[0]['point']['coordinates']
                return coordinates[0], coordinates[1]  # Latitude, Longitude
            else:
                return None, None
        else:
            return None, None
    else:
        return None, None

# Function to geocode address using ArcGIS
def geocode_gis(address):
    """
    This function simply applies ArcGIS API to geocode address and returns corresponding Latitude and Longitude.
    :param address: Full address details including city, state, zip code and country code.
    :return: Lat. and long. for the given address
    """
    try:
        result = geocode(address)[0]
        return result['location']['x'], result['location']['y']
    except Exception as e:
        print(f"Error geocoding {address}: {e}")
        return None, None
def geocode_addresses(df, geocoding=config.get('Geocoding', 'type')):
    """
    This is an implementation of the geocoding function that takes a dataframe and a geocoding type as input.
    :param df: DataFrame containing the address details.
    :param geocoding: Type of geocoding API to be used.
    :return: New dataframe containing the geocoding results as new columns, besides the original dataframe data.
    """

    addresses = df[['CLIENT_NAME', 'ADDRESS_LINE_1', 'ADDRESS_LINE_2', 'CITY_NAME', 'ZIP_CODE', 'STATE_NAME',
                    'COUNTRY_NAME']].apply(lambda row: ', '.join(row.dropna().astype(str)), axis=1)

    longitudes = []
    latitudes = []

    if geocoding == 'gis':
        logger.info(f"Using ArcGIS Geocoding API")
        gis = GIS(api_key=os.environ.get('GIS_MAPS_API_KEY'))

        logger.info(f"Starting geocoding of {len(addresses)} addresses.")
        for address in addresses:
            if len(address) > 200:
                logger.error(f"The following address is too long to be processed: {address}.")
                longitudes.append("Geocoding Error")
                latitudes.append("Geocoding Error")

            else:
                lng, lat = geocode_gis(address)
                longitudes.append(lng)
                latitudes.append(lat)

        logger.info(f"Processed {len(addresses)} addresses using ArcGIS API.")

        # Add longitude and latitude to the DataFrame
        df['LATITUDE'] = latitudes
        df['LONGITUDE'] = longitudes

        return df

    elif geocoding == 'google':
        logger.info(f"Using google maps Geocoding API")
        gmaps = googlemaps.Client(key=os.environ.get('GOOGLE_MAPS_API_KEY'))

        logger.info(f"Starting concurrent geocoding of {len(addresses)} addresses.")
        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(lambda addr: geocode_google(addr, gmaps), addresses))
        logger.info(f"Processed {len(addresses)} addresses using google maps API.")
        for (lon, lat), index in zip(results, df.index):
            df.at[index, 'LONGITUDE'] = lon
            df.at[index, 'LATITUDE'] = lat

        return df

    elif geocoding == 'bing':
        bmaps = os.environ.get('BING_MAPS_API_KEY')

        try:
            for address in addresses:
                lat, lng = geocode_bing(address)
                longitudes.append(lng)
                latitudes.append(lat)
                df['LATITUDE'] = latitudes
                df['LONGITUDE'] = longitudes

            return df

        except HTTPError as http_err:
            if http_err.response.status_code == 403:
                # Handle Bing API limit reached
                logging.error("API limit reached. Please try again later.")
                return df

            elif http_err.response.status_code == 429:
                # Handle Bing API limit reached
                logging.error("API limit reached. Please try again later.")
                return df
            else:
                # Handle other HTTP errors
                logging.error(f"HTTP error occurred: {http_err}")
                return df
        except Exception as err:
            # Handle other exceptions
            logging.error(f"An error occurred: {err}")
            return df

    else:
        df['Longitude'] = None
        df['Latitude'] = None
        return df