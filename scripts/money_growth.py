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
# # Monetary growth
# This notebook has the following goals:
# - To show the differences in the growth of the quantity of money in different economies.
# - To establish a relationship between growth of the quantity of money, velocity of money and inflation.
# The following monetary areas will be studied: the United States, the euro area, Switzerland, the United Kingdom and Japan.

# %%
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import pandas as pd
import numpy as np
from typing import Final, Literal

from utils import plot_recessions

plt.rcParams["font.family"] = "serif"
plt.rcParams["mathtext.fontset"] = "dejavuserif"

PLOTS_PATH: Final[str] = "figures"

# %%
df_us = pd.read_pickle("data/us.pickle")
df_eu = pd.read_pickle("data/eu.pickle")
df_ch = pd.read_pickle("data/ch.pickle")
df_uk = pd.read_pickle("data/uk.pickle")
df_jp = pd.read_pickle("data/jp.pickle")

# %% [markdown]
# ## Exploratory Data Analysis

# %%
fig, ax = plt.subplots()

df_us["cpi"].pct_change(periods=12).plot(ax=ax, label="US")
df_eu["cpi"].pct_change(periods=12).plot(ax=ax, label="EU")
df_uk["cpi"].pct_change(periods=12).plot(ax=ax, label="UK")
df_ch["cpi"].pct_change(periods=12).plot(ax=ax, label="CH")
df_jp["cpi"].pct_change(periods=12).plot(ax=ax, label="JP")

plot_recessions(ax)

ax.set_xlim(["2015", "2024-06"])
ax.set_ylim([-0.025, 0.15])
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.grid(which="major", axis="y", linestyle="--")  #  or bothdd
ax.set_title("Annual inflation rate (monthly, YoY)")
ax.set_xlabel("")
ax.set_ylabel("CPI [%]")
ax.legend()
fig.savefig(f"{PLOTS_PATH}/inflation.eps", bbox_inches="tight")
fig.savefig(f"{PLOTS_PATH}/inflation.svg", bbox_inches="tight")

# %%
fig, ax = plt.subplots()

df_us["m3"].pct_change(periods=12, fill_method=None).plot(ax=ax, label="US")
df_eu["m3"].pct_change(periods=12, fill_method=None).plot(ax=ax, label="EU")
df_uk["m3"].pct_change(periods=12, fill_method=None).plot(ax=ax, label="UK")
df_ch["m3"].pct_change(periods=12, fill_method=None).plot(ax=ax, label="CH")
df_jp["m3"].pct_change(periods=12, fill_method=None).plot(ax=ax, label="JP")
plot_recessions(ax)

ax.set_xlim(["2015", "2024-06"])
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.grid(which="major", axis="y", linestyle="--")  #  or bothdd
ax.set_xlabel("")
ax.set_ylabel("Anuual change [%]")
# ax.set_title('Cambios en la oferta de dinero para diferentes áreas monetarias');
ax.legend()
fig.savefig(f"{PLOTS_PATH}/money-supply.eps", bbox_inches="tight")
fig.savefig(f"{PLOTS_PATH}/money-supply.svg", bbox_inches="tight")

# %% [markdown]
# ## Money velocity growth analysis

# %% [markdown]
# ### Hypothesis test
#
# $H_0$: Mean value of $\Delta\log(v_t)$ is zero.
#
# Null hypothesis is rejected with a significance level of 99%.

# %%
df = df_us
region: Literal["us", "eu", "ch", "uk", "jp"] = "us"

df_deltaLog = df[["m3", "gdp", "v", "cpi"]].dropna().apply(np.log).diff(periods=12).join(df["recession"])

regions: dict[str, str] = {
    "us": "United States",
    "eu": "Euro area",
    "ch": "Switzerland",
    "uk": "United Kingdom",
    "jp": "Japan",
}

fig, ax = plt.subplots()
df_deltaLog[["m3", "gdp", "v", "cpi"]].plot(ax=ax)
plot_recessions(ax)

ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.grid(which="major", axis="y", linestyle="--")  #  or both
ax.set_xlabel("")
ax.set_ylabel(r"$\Delta\log(x_i)$")
ax.set_title(regions[region])

fig.savefig(f"{PLOTS_PATH}/magnitudes-{region}.eps", bbox_inches="tight")
fig.savefig(f"{PLOTS_PATH}/magnitudes-{region}.svg", bbox_inches="tight")

# %%
from scipy.stats import t

mu_0 = 0
mean_yi = df_deltaLog["v"].mean()
sm_yi = df_deltaLog["v"].std()
T = df_deltaLog["v"].count()  #  Excluye valores N/A
sm_medias = sm_yi / np.sqrt(T)

t_score = (mean_yi - mu_0) / sm_medias
p_value = 2 * t.sf(np.abs(t_score), T - 1)
print(f"p-value: {p_value:.5f}")
# Se rechaza H0: mu0=0 al 99%

print(f"mu = {mean_yi * 100:.2f}%")
print(f"mu = {df_deltaLog.loc[:'2019-12-31', 'v'].mean() * 100:.2f}% (hasta 2019.IV)")

print(f"std = {sm_yi * 100:.2f}%")
print(f"std = {df_deltaLog.loc[:'2019-12-31', 'v'].std() * 100:.2f}% (hasta 2019.IV)")

# %%
# %% Augmented Dickey-Fuller
from statsmodels.tsa.stattools import adfuller

result = adfuller(df_deltaLog["v"].dropna(), regression="c", autolag="AIC")
print("Augmented Dickey-Fuller")
print(f"\tADF statistic: {result[0]}")
print(f"\tp-value: {result[1]}")
print("Critical Values:")
for key, value in result[4].items():
    print("\t%s: %.3f" % (key, value))

# %%
from statsmodels.tsa.stattools import kpss

result = kpss(df_deltaLog["v"].dropna(), regression="c", nlags="auto")
print("KPSS")
print(f"\tKPSS statistic: {result[0]}")
print(f"\tp-value: {result[1]}")
print("Critical Values:")
for key, value in result[3].items():
    print("\t%s: %.3f" % (key, value))

# %% [markdown]
# ### Markov switching model
#
# $\Delta\log v_t = \mu_{S_t} + \beta d_\text{recession} + \varepsilon_t$
#
# $\varepsilon_t \sim N(0, \sigma^2)$
#
# $S_t\in \{0,1\}$
#
# $P(S_t=s_t|S_{t-1}=s_{t-1})=
# \begin{bmatrix}
#   p_{00} & p_{10}\\
#   1-p_{00} & 1-p_{10}
# \end{bmatrix}$
#
# $p_ij$ is the probability of transitioning from regime $i$ to regime $j$.
#
# References:
# - Reference: https://stackoverflow.com/questions/42796743/python-statsmodel-tsa-markovautoregression-using-current-real-gnp-gdp-data
# - Reference: https://nbviewer.org/gist/ChadFulton/a5d24d32ba3b7b2e381e43a232342f1f
# - statsmodels: https://www.statsmodels.org/dev/generated/statsmodels.tsa.regime_switching.markov_autoregression.MarkovAutoregression.html
# - statsmodels: https://www.statsmodels.org/dev/examples/notebooks/generated/markov_autoregression.html
# - statsmodels: https://www.statsmodels.org/dev/examples/notebooks/generated/markov_regression.html
# - mswitch (Stata): https://www.stata.com/manuals14/tsmswitch.pdf
#

# %%
import statsmodels.api as sm


df = df_deltaLog.dropna()
y = df["v"].astype(float)
X = df[["recession"]].astype(float)

max_lags = 3

for i in range(1, max_lags + 1):
    X[f"recession_lag_{i}"] = df["recession"].shift(i)

y = y[max_lags:]
X = X.iloc[max_lags:]

# X = sm.add_constant(X) # Not to be included if the option trend='c' is specified at MarkovRegression

model = sm.tsa.MarkovRegression(
    y,
    k_regimes=2,
    exog=X,
    switching_exog=False,
    # switching_trend=False,
    trend="c",
)
res = model.fit()

print(f"Estimated duration: {res.expected_durations}")
res.summary()

# %%
y_hat = res.predict()
y_mean = y.mean()

sst = ((y - y_mean) ** 2).sum()
sse = ((y - y_hat) ** 2).sum()
ssr = ((y_hat - y_mean) ** 2).sum()
r2 = 1 - sse / sst

print(f"R^2 = {r2:.4f}")

print(f"SST = {sst:.4f}")
print(f"SSE = {sse:.4f}")
print(f"SSR = {ssr:.4f}")

# %%
res.params.map("{:,.3f}".format) + res.pvalues.map(" ({:,.3f})".format)

print("Numerical values:")
params = ["const[0]", "const[1]", "x1[1]", "p[0->0]", "p[1->0]"]
for param in params:
    print(f"{res.params[param]:.3f}\n({res.pvalues[param]:.3f})")
print(f"{r2:.3f}")

# %%
fig, ax = plt.subplots(2)
ax[0].set_title(regions[region])
res.smoothed_marginal_probabilities[1].plot(
    ylabel="Prob. of being in $S_t=1$", ax=ax[0]
)
plot_recessions(ax[0])
ax[0].set_xlabel("")

y_hat.plot(ax=ax[1], label="Prediction")
y.plot(ax=ax[1], label="Real")
plot_recessions(ax[1])

ax[1].grid(which="major", axis="y", linestyle="--")  #  or both
ax[1].set_xlabel("")
ax[1].set_ylabel(r"$\Delta\log v_t$")
ax[1].legend()

fig.savefig(f"{PLOTS_PATH}/markov-v-{region}.eps", bbox_inches="tight")
fig.savefig(f"{PLOTS_PATH}/markov-v-{region}.svg", bbox_inches="tight")
print(f"R^2 = {r2:.4f}")

# %% [markdown]
# ## Inflation switching model

# %%
y = df_deltaLog.dropna()["cpi"].astype(float)
X = df_deltaLog.dropna()[["v", "m3"]].astype(float)

max_lags = 2

for i in range(1, max_lags + 1):
    X[f"v_lag_{i}"] = X["v"].shift(i)

for i in range(1, max_lags + 1):
    X[f"m3_lag_{i}"] = X["m3"].shift(i)

y = y[max_lags:]
X = X.iloc[max_lags:]
# X.drop('v', axis=1, inplace=True)

model = sm.tsa.MarkovRegression(
    y,
    k_regimes=2,
    exog=X,
    # switching_exog={'v': False, 'm3': True},
    switching_exog=[False] * (max_lags + 1) + [True] * (max_lags + 1),
    # switching_trend=False,
    trend="c",
)
res = model.fit()

print(f"Estimated duration: {res.expected_durations}")
res.summary()

# %%
y_hat = res.predict()
y_mean = y.mean()

sst = ((y - y_mean) ** 2).sum()
sse = ((y - y_hat) ** 2).sum()
ssr = ((y_hat - y_mean) ** 2).sum()
r2 = 1 - sse / sst

print(f"R^2 = {r2:.4f}")

print(f"SST = {sst:.4f}")
print(f"SSE = {sse:.4f}")
print(f"SSR = {ssr:.4f}")

# %%
# print(res.params.map('{:,.3f}'.format) + res.pvalues.map(' ({:,.3f})'.format))

print("Numerical values:")
params = [
    "const[0]",
    "x4[0]",
    "x5[0]",
    "x6[0]",
    "const[1]",
    "x4[1]",
    "x5[1]",
    "x6[1]",
    "x1[1]",
    "x2[1]",
    "x3[1]",
    "p[0->0]",
    "p[1->0]",
]
for param in params:
    print(f"{res.params[param]:.3f}\n({res.pvalues[param]:.3f})")
print(f"{r2:.3f}")

# %%
fig, ax = plt.subplots(2)
ax[0].set_title(regions[region])
res.smoothed_marginal_probabilities[1].plot(
    ylabel="Prob. of being in $S_t=1$", ax=ax[0]
)
plot_recessions(ax[0])
ax[0].set_xlabel("")

y_hat.plot(ax=ax[1], label="Prediction")
y.plot(ax=ax[1], label="Real")
plot_recessions(ax[1])

ax[1].grid(which="major", axis="y", linestyle="--")  #  or both
ax[1].set_xlabel("")
ax[1].set_ylabel(r"$\Delta\log P_t$")
ax[1].legend()

fig.savefig(f"{PLOTS_PATH}/markov-cpi-{region}.eps", bbox_inches="tight")
fig.savefig(f"{PLOTS_PATH}/markov-cpi-{region}.svg", bbox_inches="tight")
print(f"R^2 = {r2:.4f}")

# %%
