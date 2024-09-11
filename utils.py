import urllib
import pandas as pd
import requests
import io
import json


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


def get_boe_data(series_codes):
    url_endpoint = 'http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes'

    params = {
        'Datefrom': '01/Jan/1963',
        'Dateto': '01/Dec/2024',
        'SeriesCodes': series_codes,  # ','.join(series_codes)
        'CSVF': 'TN',
        'UsingCodes': 'Y',
        'VPD': 'Y',
        'VFD': 'N'
    }

    url = url_endpoint + '&' + urllib.parse.urlencode(params, safe='()')

    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/54.0.2840.90 '
        'Safari/537.36'
    }
    df = pd.read_csv(url, storage_options=headers, parse_dates=['DATE'])
    df.rename(columns={'DATE': 'Date'}, inplace=True)

    # Always returns last day of the month
    df['Date'] = df['Date'] + pd.Timedelta('1 day')
    df.set_index(['Date'], inplace=True)

    return df[params['SeriesCodes']]


def get_imf_indicators():
    response = requests.get(
        url="https://www.imf.org/external/datamapper/api/v1/indicators"
    )
    response_body = json.loads(response.text)
    indicators = [
        {"id": key, **values} for key, values in response_body["indicators"].items()
    ]
    indicators_df = pd.DataFrame.from_records(indicators)
    return indicators_df
