import urllib
from matplotlib.figure import Figure
import pandas as pd
import requests
import io
import json
from typing import Final, Mapping
from fredapi import Fred
from matplotlib.axes import Axes
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from data_preparation import s_recession

fred = Fred()

HEADERS: Final[Mapping[str, str]] = {"User-Agent": "MoneyGrowth"}


def get_recession_periods(
    recession_series: pd.Series,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Convert a binary recession series into a list of (start, end) timestamp tuples."""
    recession_series = recession_series.astype(bool)
    shifted = recession_series.shift(1, fill_value=False)
    starts = (recession_series & ~shifted).index[recession_series & ~shifted]
    ends = (~recession_series & shifted).index[~recession_series & shifted]

    # Handle case where series ends during a recession
    if len(ends) < len(starts):
        ends = ends.append(pd.Index([recession_series.index[-1]]))

    return list(zip(starts, ends))


def plot_recessions(ax: Axes):
    """Plot recession periods on the given axis."""
    recession_periods = get_recession_periods(s_recession)
    for period in recession_periods:
        ax.axvspan(period[0], period[1], facecolor="lightgray")


def plot_pct_change(df: pd.DataFrame, region: str) -> tuple[Figure, Axes]:
    fig, ax = plt.subplots()

    df[["m3", "gdp", "v"]].pct_change(periods=12, fill_method=None).plot(ax=ax)
    plot_recessions(ax)

    ax.set_title(region)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.grid(which="major", axis="y", linestyle="--")  #  or both
    ax.set_xlabel("")
    ax.set_ylabel("Annual change [%]")

    ax.set_xlim([df["m3"].first_valid_index(), None])

    return fig, ax


def get_snb_data(table_id, params):
    url = (
        f"https://data.snb.ch/api/cube/{table_id}/data/csv/en?"
        + urllib.parse.urlencode(params, safe="(),")
    )

    df = pd.read_csv(url, sep=";", skiprows=2, parse_dates=["Date"], index_col="Date")
    return df


def get_ecb_data(flowRef, key, parameters={}):
    entrypoint = "https://sdw-wsrest.ecb.europa.eu/service/"  # Using protocol 'https'
    resource = "data"  # The resource for data queries is always'data'

    # Define the parameters
    # 'startPeriod': '2000-01-01',  # Start date of the time series
    # 'endPeriod': '2018-10-01'     # End of the time series

    # Construct the URL: https://sdw-wsrest.ecb.europa.eu/service/data/EXR/D.CHF.EUR.SP00.A
    request_url = entrypoint + resource + "/" + flowRef + "/" + key

    # Make the HTTP request
    response = requests.get(
        request_url, params=parameters, headers={"Accept": "text/csv"}
    )

    if response.status_code != 200:
        return None

    df = (
        pd.read_csv(
            io.StringIO(response.text),
            parse_dates=["TIME_PERIOD"],
            usecols=["TIME_PERIOD", "OBS_VALUE"],
        )
        .rename(columns={"TIME_PERIOD": "Date", "OBS_VALUE": "Value"})
        .set_index("Date")
    )

    return df["Value"]


def get_boe_data(series_codes):
    url_endpoint = (
        "http://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp?csv.x=yes"
    )

    params = {
        "Datefrom": "01/Jan/1963",
        "SeriesCodes": series_codes,  # ','.join(series_codes)
        "CSVF": "TN",
        "UsingCodes": "Y",
        "VPD": "Y",
        "VFD": "N",
    }

    url = url_endpoint + "&" + urllib.parse.urlencode(params, safe="()")

    df = pd.read_csv(url, storage_options=HEADERS, parse_dates=["DATE"])
    df.rename(columns={"DATE": "Date"}, inplace=True)

    # Always returns last day of the month
    df["Date"] = df["Date"] + pd.Timedelta("1 day")
    df.set_index(["Date"], inplace=True)

    return df[params["SeriesCodes"]]


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
