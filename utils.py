import urllib
import pandas as pd
import requests
import io
import json
from typing import Final

# https://www.nber.org/research/data/us-business-cycle-expansions-and-contractions
RECESSIONS_DATA: Final[pd.DataFrame] = pd.DataFrame([
    [pd.Timestamp('1857-04-01'), pd.Timestamp('1858-10-01')],
    [pd.Timestamp('1860-07-01'), pd.Timestamp('1861-07-01')],
    [pd.Timestamp('1865-01-01'), pd.Timestamp('1867-01-01')],
    [pd.Timestamp('1869-04-01'), pd.Timestamp('1870-10-01')],
    [pd.Timestamp('1873-07-01'), pd.Timestamp('1879-01-01')],
    [pd.Timestamp('1882-01-01'), pd.Timestamp('1885-04-01')],
    [pd.Timestamp('1887-04-01'), pd.Timestamp('1888-01-01')],
    [pd.Timestamp('1890-07-01'), pd.Timestamp('1891-04-01')],
    [pd.Timestamp('1893-01-01'), pd.Timestamp('1894-04-01')],
    [pd.Timestamp('1895-10-01'), pd.Timestamp('1897-04-01')],
    [pd.Timestamp('1899-07-01'), pd.Timestamp('1900-10-01')],
    [pd.Timestamp('1902-10-01'), pd.Timestamp('1904-07-01')],
    [pd.Timestamp('1907-04-01'), pd.Timestamp('1908-04-01')],
    [pd.Timestamp('1910-01-01'), pd.Timestamp('1911-10-01')],
    [pd.Timestamp('1913-01-01'), pd.Timestamp('1914-10-01')],
    [pd.Timestamp('1918-07-01'), pd.Timestamp('1919-01-01')],
    [pd.Timestamp('1920-01-01'), pd.Timestamp('1921-07-01')],
    [pd.Timestamp('1923-04-01'), pd.Timestamp('1924-07-01')],
    [pd.Timestamp('1926-07-01'), pd.Timestamp('1927-10-01')],
    [pd.Timestamp('1929-07-01'), pd.Timestamp('1933-01-01')],
    [pd.Timestamp('1937-04-01'), pd.Timestamp('1938-04-01')],
    [pd.Timestamp('1945-01-01'), pd.Timestamp('1945-10-01')],
    [pd.Timestamp('1948-10-01'), pd.Timestamp('1949-10-01')],
    [pd.Timestamp('1953-04-01'), pd.Timestamp('1954-04-01')],
    [pd.Timestamp('1957-07-01'), pd.Timestamp('1958-04-01')],
    [pd.Timestamp('1960-04-01'), pd.Timestamp('1961-01-01')],
    [pd.Timestamp('1969-10-01'), pd.Timestamp('1970-10-01')],
    [pd.Timestamp('1973-10-01'), pd.Timestamp('1975-01-01')],
    [pd.Timestamp('1980-01-01'), pd.Timestamp('1980-07-01')],
    [pd.Timestamp('1981-07-01'), pd.Timestamp('1982-10-01')],
    [pd.Timestamp('1990-07-01'), pd.Timestamp('1991-01-01')],
    [pd.Timestamp('2001-01-01'), pd.Timestamp('2001-10-01')],
    [pd.Timestamp('2007-10-01'), pd.Timestamp('2009-04-01')],
    [pd.Timestamp('2019-10-01'), pd.Timestamp('2020-04-01')]],
    columns=['quarter_start', 'quarter_end'])


def add_recessions(df):
    df['recession'] = 0
    for row in RECESSIONS_DATA.itertuples():
        df.loc[row.quarter_start: row.quarter_end, 'recession'] = 1


def plot_recessions(ax):
    for row in RECESSIONS_DATA.itertuples():
        ax.axvspan(row.quarter_start, row.quarter_end, facecolor='lightgray')


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
