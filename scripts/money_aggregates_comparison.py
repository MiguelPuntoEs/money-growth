# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.3
#   kernelspec:
#     display_name: .venv
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Money aggregates comparison
#
# This notebook compares different information sources for money aggregates.

# %%
import pandas as pd
import matplotlib.pyplot as plt
from fredapi import Fred
from dotenv import load_dotenv
import os
from utils import HEADERS

load_dotenv()

fred = Fred(api_key=os.environ.get("FRED_API_KEY"))

plt.rcParams["font.family"] = "serif"
plt.rcParams["mathtext.fontset"] = "dejavuserif"

# %% [markdown]
# ## United States
# ### M3

# %%
# MANM (M1) y MABM (M3)
url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MONAGG,4.0/USA.M.MABM.XDC..N...?format=csv"
s_oecd = pd.read_csv(
    url, storage_options=HEADERS, parse_dates=["TIME_PERIOD"], date_format="%Y-%m", index_col=["TIME_PERIOD"]
)["OBS_VALUE"].sort_index()
from data_preparation import _load_us_m3

s_shadowstats = _load_us_m3() * 1e3
s_fred = fred.get_series("M2NS") * 1e3

# %%
fig, ax = plt.subplots()
s_shadowstats.plot(ax=ax, label="ShadowStats")
s_oecd.plot(ax=ax, label="OECD")
s_fred.plot(ax=ax, label="FRED")

ax.set_xlabel("")
ax.set_ylabel("M3")
ax.set_title("Money aggregates comparison")
ax.grid()
ax.legend()

# %% [markdown]
# ### M1

# %%
s_fred = fred.get_series("M1NS") * 1e3

url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MONAGG,4.0/USA.M.MANM.XDC..N...?format=csv"
s_oecd = pd.read_csv(
    url, storage_options=HEADERS, parse_dates=["TIME_PERIOD"], date_format="%Y-%m", index_col=["TIME_PERIOD"]
)["OBS_VALUE"].sort_index()

# %%
fig, ax = plt.subplots()
s_fred.plot(ax=ax, label="FRED")
s_oecd.plot(ax=ax, label="OECD")

ax.set_xlabel("")
ax.set_ylabel("M1")
ax.set_title("Money aggregates comparison")
ax.grid()
ax.legend();

# %% [markdown]
# ## Japan

# %%
url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MONAGG,4.0/JPN.M.MABM.XDC..N...?format=csv"

s_oecd = pd.read_csv(
    url, storage_options=HEADERS, parse_dates=["TIME_PERIOD"], date_format="%Y-%m", index_col=["TIME_PERIOD"]
)["OBS_VALUE"].sort_index()

# %%
s_boj = (
    pd.read_csv(
        "data/m3-jp.csv",
        skiprows=3,
        names=["Date", "Value"],
        parse_dates=["Date"],
        date_format={"Date": "%Y/%m"},
        index_col="Date",
    )["Value"]
    * 100
)  # Monthly data, 100 million yen

# %%
fig, ax = plt.subplots()
s_oecd.plot(ax=ax, label="OECD")
s_boj.plot(ax=ax, label="BoJ")

ax.set_xlabel("")
ax.set_ylabel("M3")
ax.set_title("Money aggregates comparison")
ax.legend()
ax.grid()

# %% [markdown]
# ## Switzerland

# %%
url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MONAGG,4.0/CHE.M.MABM.XDC..N...?format=csv"

s_oecd = pd.read_csv(
    url, storage_options=HEADERS, parse_dates=["TIME_PERIOD"], date_format="%Y-%m", index_col=["TIME_PERIOD"]
)["OBS_VALUE"].sort_index()

# %%
from utils import get_snb_data

table_id = "snbmonagg"
params = {
    "dimSel": "D0(B),D1(GM3)",
    "fromDate": "1980-01",
}
# In CHF millions
s_snb = get_snb_data(table_id, params)["Value"]  # Monthly data
s_snb

# %%
fig, ax = plt.subplots()
s_oecd.plot(ax=ax, label="OECD")
(s_snb).plot(ax=ax, label="SNB")

ax.set_xlabel("")
ax.set_ylabel("M3")
ax.set_title("Money aggregates comparison")
ax.legend()
ax.grid()

# %% [markdown]
# ## United Kingdom

# %%
url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_MONAGG,4.0/GBR.M.MABM.XDC..N...?format=csv"

s_oecd = pd.read_csv(
    url, storage_options=HEADERS, parse_dates=["TIME_PERIOD"], date_format="%Y-%m", index_col=["TIME_PERIOD"]
)["OBS_VALUE"].sort_index()

# %%
from utils import get_boe_data

s_boe = get_boe_data("LPMAUYN")

# %%
fig, ax = plt.subplots()
s_oecd.plot(ax=ax, label="OECD")
s_boe.plot(ax=ax, label="BoE")

ax.set_xlabel("")
ax.set_ylabel("M3")
ax.set_title("Money aggregates comparison")
ax.legend()
ax.grid()

# %%
