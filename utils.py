import urllib
import pandas as pd
import requests
import io


def get_snb_data(table_id, params):
    url = f"https://data.snb.ch/api/cube/{table_id}/data/csv/en?" + \
        urllib.parse.urlencode(params, safe='(),')

    df = pd.read_csv(url,
                     sep=";",
                     skiprows=2,
                     parse_dates=["Date"],
                     index_col='Date')
    return df


def get_ecb_data(flowRef, key, parameters={}):
    entrypoint = 'https://sdw-wsrest.ecb.europa.eu/service/'  # Using protocol 'https'
    resource = 'data'           # The resource for data queries is always'data'

    # Define the parameters
    # 'startPeriod': '2000-01-01',  # Start date of the time series
    # 'endPeriod': '2018-10-01'     # End of the time series

    # Construct the URL: https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D.CHF.EUR.SP00.A
    request_url = entrypoint + resource + '/' + flowRef + '/' + key

    # Make the HTTP request
    response = requests.get(request_url,
                            params=parameters,
                            headers={'Accept': 'text/csv'})

    if response.status_code != 200:
        return None

    df = pd.read_csv(io.StringIO(response.text), parse_dates=[
                     'TIME_PERIOD'],
                     usecols=['TIME_PERIOD', 'OBS_VALUE'],
                     ).rename(columns={'TIME_PERIOD': 'Date', 'OBS_VALUE': 'Value'}).set_index('Date')

    return df['Value']
