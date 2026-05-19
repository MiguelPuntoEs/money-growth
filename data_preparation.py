from fredapi import Fred
import pandas as pd

from utils import get_boe_data, get_ecb_data, get_snb_data, s_recession

fred = Fred()


US_M3_XLSX = "private/USA WMM 2603 260320 TC-1.xlsx"
_MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _load_us_m3() -> pd.Series:
    """Parse the discontinued FRED M3 series + ShadowStats extension from xlsx.

    Layout: year column populated only on Jan rows; month name in next col; M3 level in next.
    """
    df = pd.read_excel(
        US_M3_XLSX,
        sheet_name="Sheet1",
        skiprows=19,
        usecols=[1, 2, 3],
        names=["year", "month", "m3"],
    )
    df["year"] = pd.to_numeric(df["year"], errors="coerce").ffill().astype(int)
    df["month"] = df["month"].astype(str).str.strip()
    df = df[df["month"].isin(_MONTH_NAMES)].dropna(subset=["m3"]).copy()
    df["Date"] = pd.to_datetime(
        df["year"].astype(str) + "-" + df["month"], format="%Y-%b"
    )
    return df.set_index("Date")["m3"].astype(float).sort_index().rename("m3")


def get_us_data() -> pd.DataFrame:
    """Fetch and prepare US economic data for analysis."""

    # M3: Monthly data, USD billions
    s_m3 = _load_us_m3()
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
