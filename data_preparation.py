from fredapi import Fred
import pandas as pd

from utils import get_boe_data, get_ecb_data, get_snb_data, s_recession

fred = Fred()


def get_us_data() -> pd.DataFrame:
    """Fetch and prepare US economic data for analysis."""

    # M3: Monthly data, USD billions
    s_m3 = pd.read_csv(
        "data/m3-us.csv", sep=";", parse_dates=["Date"], index_col="Date"
    )["m3"]
    # GDP: Quarterly data transformed to monthly
    s_gdp = fred.get_series("GDP").rename("gdp").resample("MS").ffill()
    # CPI: Monthly data
    s_cpi = fred.get_series("CPIAUCSL").rename("cpi")

    df = pd.concat([s_m3, s_gdp, s_cpi, s_recession], axis=1)
    df["v"] = df["gdp"] / df["m3"]

    return df


def get_eu_data() -> pd.DataFrame:
    """Fetch and prepare EU economic data for analysis."""

    # M3: Monthly data
    s_m3 = get_ecb_data("BSI", "M.U2.N.V.M30.X.1.U2.2300.Z01.E").rename("m3")

    # GDP: Quarterly data transformed to monthly
    s_gdp = (
        get_ecb_data("MNA", "Q.N.U2.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.V.N")
        .rename("gdp")
        .resample("MS")
        .ffill()
    )
    # CPI: Monthly data
    s_cpi = get_ecb_data("ICP", "M.U2.N.000000.4.INX").rename("cpi")

    df = pd.concat([s_m3, s_gdp, s_cpi, s_recession], axis=1)
    df["v"] = df["gdp"] / df["m3"]

    return df


def get_ch_data() -> pd.DataFrame:
    """Fetch and prepare Swiss economic data for analysis."""

    table_id = "snbmonagg"
    params = {
        "dimSel": "D0(B),D1(GM3)",
        "fromDate": "1980-01",
    }

    # M3: Monthly data
    s_m3 = get_snb_data(table_id, params)["Value"].rename("m3")

    table_id = "gdpap"
    params = {
        "dimSel": "D0(WMF),D1(BBIPS)",
        "fromDate": "1980-01",
    }

    # GDP: Quarterly data transformed to monthly
    s_gdp = get_snb_data(table_id, params)["Value"].rename("gdp").resample("MS").ffill()

    table_id = "plkopr"
    params = {
        "dimSel": "D0(LD2010100)",
        "fromDate": "1980-01",
    }
    # CPI: Monthly data
    s_cpi = get_snb_data(table_id, params)["Value"].rename("cpi")

    df = pd.concat([s_m3, s_gdp, s_cpi, s_recession], axis=1)
    df["v"] = df["gdp"] / df["m3"]

    return df


def get_uk_data() -> pd.DataFrame:
    """Fetch and prepare UK economic data for analysis."""

    # M4x: Monthly data
    s_m3 = get_boe_data("LPMAUYN").rename("m3")

    # GDP: Quarterly data transformed to monthly
    s_gdp = (
        pd.read_csv(
            "data/gdp-gb.csv",
            skiprows=84,
            names=["Date", "Value"],
            parse_dates=["Date"],
            index_col="Date",
        )["Value"]
        .rename("gdp")
        .resample("MS")
        .ffill()
    )

    # CPI: Monthly data
    s_cpi = pd.read_csv(
        "data/cpi-gb.csv",
        skiprows=190,
        names=["Date", "Value"],
        parse_dates=["Date"],
        index_col="Date",
    )["Value"].rename("cpi")

    df = pd.concat([s_m3, s_gdp, s_cpi, s_recession], axis=1)
    df["v"] = df["gdp"] / df["m3"]

    return df


def get_jp_data() -> pd.DataFrame:
    # M3: Monthly data
    s_m3 = pd.read_csv(
        "data/m3-jp.csv",
        skiprows=3,
        names=["Date", "Value"],
        parse_dates=["Date"],
        date_format={"Date": "%Y/%m"},
        index_col="Date",
    )["Value"].rename("m3")

    # GDP: Quarterly data transformed to monthly
    s_gdp = fred.get_series("JPNNGDP").rename("gdp").resample("MS").ffill()

    # CPI: Monthly data
    s_cpi = pd.read_csv(
        "data/jp-cpi-e-stat.csv", parse_dates=["Date"], index_col="Date"
    )[r"CPI (2020=100)"].rename("cpi")  # Monthly data

    df = pd.concat([s_m3, s_gdp, s_cpi, s_recession], axis=1)
    df["v"] = df["gdp"] / df["m3"]

    return df
