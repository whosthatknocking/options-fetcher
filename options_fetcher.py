import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from calendar import monthrange
from scipy.stats import norm

# ── CONFIG ─────────────────────────────────────────────
TICKERS = ["TSLA", "NVDA", "UBER", "MSFT", "GOOGL", "ORCL", "PLTR"]
MIN_BID = 0.50
MIN_OPEN_INTEREST = 100
MIN_VOLUME = 10
MAX_SPREAD_PCT_OF_MID = 0.25
RISK_FREE_RATE = 0.045
DATA_SOURCE = "yfinance"
SCRIPT_VERSION = "2026-03-19.1"
STALE_QUOTE_SECONDS = 15 * 60

# Build a rolling expiration cutoff three months ahead.
today = datetime.today().date()
year = today.year
month = today.month + 3
if month > 12:
    month -= 12
    year += 1
_, last_day = monthrange(year, month)
MAX_EXPIRATION = f"{year}-{month:02d}-{last_day:02d}"

print(f"Today: {today}")
print(f"Max expiration: {MAX_EXPIRATION}")


def coerce_float(value):
    """Convert scalar inputs to float while keeping missing values as NaN."""
    return pd.to_numeric(value, errors="coerce")


def normalize_timestamp(value):
    """Convert vendor timestamps to timezone-aware UTC pandas timestamps."""
    if value is None or pd.isna(value):
        return pd.NaT

    if isinstance(value, (int, float, np.integer, np.floating)):
        return pd.to_datetime(value, unit="s", utc=True, errors="coerce")

    ts = pd.to_datetime(value, utc=True, errors="coerce")
    return ts


# ── VECTOR GREEKS ──────────────────────────────────────
def compute_greeks(df, underlying_price, risk_free_rate):
    # Replace zero IVs before vectorized math to avoid divide-by-zero blowups.
    strike = df["strike"].to_numpy(dtype=float)
    time_to_expiration = df["time_to_expiration_years"].to_numpy(dtype=float)
    sigma = df["implied_volatility"].replace(0, np.nan).fillna(0.3).to_numpy(dtype=float)

    valid = (underlying_price > 0) & (strike > 0) & (time_to_expiration > 0) & (sigma > 0)

    d1 = np.full(len(df), np.nan)
    d2 = np.full(len(df), np.nan)

    d1[valid] = (
        np.log(underlying_price / strike[valid])
        + (risk_free_rate + 0.5 * sigma[valid] ** 2) * time_to_expiration[valid]
    ) / (sigma[valid] * np.sqrt(time_to_expiration[valid]))
    d2[valid] = d1[valid] - sigma[valid] * np.sqrt(time_to_expiration[valid])

    n_d1 = norm.pdf(d1)
    n_d2 = norm.cdf(d2)
    n_d1_cdf = norm.cdf(d1)

    is_call = df["option_type"] == "call"
    is_put = ~is_call
    valid_calls = valid & is_call.to_numpy()
    valid_puts = valid & is_put.to_numpy()

    delta = np.full(len(df), np.nan)
    delta[valid_calls] = n_d1_cdf[valid_calls]
    delta[valid_puts] = n_d1_cdf[valid_puts] - 1

    gamma = np.full(len(df), np.nan)
    gamma[valid] = n_d1[valid] / (underlying_price * sigma[valid] * np.sqrt(time_to_expiration[valid]))

    vega = np.full(len(df), np.nan)
    vega[valid] = underlying_price * n_d1[valid] * np.sqrt(time_to_expiration[valid]) / 100

    theta = np.full(len(df), np.nan)
    theta[valid_calls] = (
        -(underlying_price * n_d1[valid_calls] * sigma[valid_calls])
        / (2 * np.sqrt(time_to_expiration[valid_calls]))
        - risk_free_rate
        * strike[valid_calls]
        * np.exp(-risk_free_rate * time_to_expiration[valid_calls])
        * n_d2[valid_calls]
    )
    theta[valid_puts] = (
        -(underlying_price * n_d1[valid_puts] * sigma[valid_puts])
        / (2 * np.sqrt(time_to_expiration[valid_puts]))
        + risk_free_rate
        * strike[valid_puts]
        * np.exp(-risk_free_rate * time_to_expiration[valid_puts])
        * (1 - n_d2[valid_puts])
    )

    df["delta"] = delta
    df["delta_abs"] = np.abs(delta)
    df["gamma"] = gamma
    df["vega"] = vega
    df["theta"] = theta / 365
    df["delta_itm_proxy"] = np.where(is_call, df["delta"], df["delta_abs"])
    df["has_valid_greeks"] = valid

    return df


# ── LADDER ─────────────────────────────────────────────
def assign_bucket(days_to_expiration):
    if days_to_expiration <= 10:
        return "week_1"
    if days_to_expiration <= 18:
        return "week_2"
    if days_to_expiration <= 26:
        return "week_3"
    return "week_4"


def coerce_option_frame(df, underlying_price, expiration_date, option_type, ticker, fetched_at):
    """Normalize vendor columns and attach derived metrics used by downstream tools."""
    df = df.copy()

    df = df.rename(
        columns={
            "contractSymbol": "contract_symbol",
            "lastTradeDate": "option_quote_time",
            "lastPrice": "last_trade_price",
            "openInterest": "open_interest",
            "impliedVolatility": "implied_volatility",
            "inTheMoney": "is_in_the_money",
            "percentChange": "percent_change",
            "contractSize": "contract_size",
        }
    )

    expiration_ts = pd.Timestamp(expiration_date)
    days_to_expiration = (expiration_ts.date() - today).days
    time_to_expiration_years = days_to_expiration / 365.0

    df["option_type"] = option_type
    df["underlying_symbol"] = ticker
    df["expiration_date"] = expiration_date
    df["days_to_expiration"] = days_to_expiration
    df["time_to_expiration_years"] = time_to_expiration_years
    df["fetched_at"] = fetched_at
    df["data_source"] = DATA_SOURCE
    df["script_version"] = SCRIPT_VERSION
    df["risk_free_rate_used"] = RISK_FREE_RATE
    df["underlying_price"] = underlying_price

    numeric_columns = [
        "bid",
        "ask",
        "strike",
        "open_interest",
        "volume",
        "last_trade_price",
        "implied_volatility",
        "change",
        "percent_change",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    df["option_quote_time"] = pd.to_datetime(df["option_quote_time"], utc=True, errors="coerce")

    df["has_valid_underlying"] = underlying_price > 0
    df["has_valid_strike"] = df["strike"] > 0
    df["bid_le_ask"] = df["bid"] <= df["ask"]
    df["has_nonzero_bid"] = df["bid"] > 0
    df["has_nonzero_ask"] = df["ask"] > 0
    df["has_crossed_or_locked_market"] = (
        df["bid"].notna() & df["ask"].notna() & (df["bid"] >= df["ask"])
    )
    df["has_valid_quote"] = (
        df["bid"].notna()
        & df["ask"].notna()
        & (df["bid"] >= 0)
        & (df["ask"] >= 0)
        & df["bid_le_ask"]
    )
    df["has_valid_iv"] = df["implied_volatility"] > 0

    df["mark_price_mid"] = np.where(df["has_valid_quote"], (df["bid"] + df["ask"]) / 2, np.nan)
    df["bid_ask_spread"] = np.where(df["has_valid_quote"], df["ask"] - df["bid"], np.nan)
    df["bid_ask_spread_pct_of_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["bid_ask_spread"] / df["mark_price_mid"],
        np.nan,
    )
    df["spread_to_strike_pct"] = np.where(
        df["strike"] > 0,
        df["bid_ask_spread"] / df["strike"],
        np.nan,
    )
    df["spread_to_bid_pct"] = np.where(
        df["bid"] > 0,
        df["bid_ask_spread"] / df["bid"],
        np.nan,
    )
    df["oi_to_volume_ratio"] = np.where(
        df["volume"] > 0,
        df["open_interest"] / df["volume"],
        np.nan,
    )

    df["strike_minus_spot"] = df["strike"] - underlying_price
    df["strike_vs_spot_pct"] = np.where(
        underlying_price > 0,
        df["strike_minus_spot"] / underlying_price,
        np.nan,
    )
    df["strike_distance_pct"] = np.abs(df["strike_vs_spot_pct"])

    call_itm_amount = np.maximum(underlying_price - df["strike"], 0)
    put_itm_amount = np.maximum(df["strike"] - underlying_price, 0)
    df["itm_amount"] = np.where(df["option_type"] == "call", call_itm_amount, put_itm_amount)
    df["otm_pct"] = np.where(
        df["option_type"] == "call",
        np.maximum(df["strike"] - underlying_price, 0) / underlying_price,
        np.maximum(underlying_price - df["strike"], 0) / underlying_price,
    )

    df["intrinsic_value"] = df["itm_amount"]
    df["extrinsic_value_bid"] = df["bid"] - df["intrinsic_value"]
    df["extrinsic_value_mid"] = df["mark_price_mid"] - df["intrinsic_value"]
    df["extrinsic_value_ask"] = df["ask"] - df["intrinsic_value"]
    df["extrinsic_pct_mid"] = np.where(
        df["mark_price_mid"] > 0,
        df["extrinsic_value_mid"] / df["mark_price_mid"],
        np.nan,
    )
    df["has_negative_extrinsic_mid"] = df["extrinsic_value_mid"] < 0

    df["premium_reference_price"] = df["mark_price_mid"].fillna(df["bid"]).fillna(df["last_trade_price"])
    df["premium_reference_method"] = np.select(
        [
            df["mark_price_mid"].notna(),
            df["bid"].notna(),
            df["last_trade_price"].notna(),
        ],
        ["mid", "bid", "last_trade_price"],
        default="unavailable",
    )

    df["premium_to_strike"] = np.where(
        df["strike"] > 0,
        df["premium_reference_price"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_bid"] = np.where(
        df["strike"] > 0,
        df["bid"] / df["strike"],
        np.nan,
    )
    df["premium_to_strike_annualized"] = np.where(
        df["time_to_expiration_years"] > 0,
        df["premium_to_strike"] / df["time_to_expiration_years"],
        np.nan,
    )
    df["premium_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["premium_reference_price"] / df["days_to_expiration"],
        np.nan,
    )

    df = compute_greeks(df, underlying_price, RISK_FREE_RATE)

    df["theta_to_premium_ratio"] = np.where(
        df["premium_reference_price"] > 0,
        np.abs(df["theta"]) / df["premium_reference_price"],
        np.nan,
    )
    df["vega_per_day"] = np.where(
        df["days_to_expiration"] > 0,
        df["vega"] / df["days_to_expiration"],
        np.nan,
    )
    df["break_even_if_short"] = np.where(
        df["option_type"] == "call",
        df["strike"] + df["premium_reference_price"],
        df["strike"] - df["premium_reference_price"],
    )

    df["quote_age_seconds"] = (fetched_at - df["option_quote_time"]).dt.total_seconds()
    df["is_stale_quote"] = np.where(
        df["quote_age_seconds"].notna(),
        df["quote_age_seconds"] > STALE_QUOTE_SECONDS,
        pd.NA,
    )

    df["days_bucket"] = df["days_to_expiration"].apply(assign_bucket)
    df["near_expiry_near_money_flag"] = (
        (df["days_to_expiration"] <= 14) & (df["strike_distance_pct"] <= 0.03)
    )
    df["is_wide_market"] = df["bid_ask_spread_pct_of_mid"] > MAX_SPREAD_PCT_OF_MID
    df["passes_primary_screen"] = (
        (df["bid"] >= MIN_BID)
        & (df["bid_ask_spread_pct_of_mid"] < MAX_SPREAD_PCT_OF_MID)
        & (df["open_interest"] > MIN_OPEN_INTEREST)
        & (df["volume"] > MIN_VOLUME)
    )
    df["quote_quality_score"] = (
        df["has_valid_quote"].astype(int)
        + df["has_nonzero_bid"].astype(int)
        + df["has_nonzero_ask"].astype(int)
        + df["has_valid_iv"].astype(int)
        + df["has_valid_greeks"].astype(int)
        + (~df["has_crossed_or_locked_market"]).astype(int)
        + (df["is_stale_quote"] == False).fillna(False).astype(int)
    )

    return df


def get_underlying_snapshot(stock):
    """Load the underlying snapshot once per ticker and reuse it for each expiration."""
    fast_info = getattr(stock, "fast_info", {}) or {}
    try:
        info = stock.info
    except Exception:
        info = {}

    last_price = coerce_float(
        fast_info.get("lastPrice")
        or info.get("regularMarketPrice")
        or info.get("previousClose")
    )
    previous_close = coerce_float(
        fast_info.get("previousClose")
        or info.get("previousClose")
    )
    if pd.notna(last_price) and pd.notna(previous_close) and previous_close > 0:
        underlying_day_change_pct = (last_price - previous_close) / previous_close
    else:
        underlying_day_change_pct = np.nan

    underlying_price_time = normalize_timestamp(info.get("regularMarketTime"))

    return {
        "underlying_price": last_price,
        "underlying_price_time": underlying_price_time,
        "underlying_currency": info.get("currency") or fast_info.get("currency"),
        "underlying_market_state": info.get("marketState"),
        "underlying_day_change_pct": underlying_day_change_pct,
    }


# ── FETCH DATA ─────────────────────────────────────────
def fetch_chain(ticker):
    try:
        fetched_at = pd.Timestamp.now(tz=timezone.utc)
        stock = yf.Ticker(ticker)
        snapshot = get_underlying_snapshot(stock)
        underlying_price = snapshot["underlying_price"]

        if pd.isna(underlying_price) or underlying_price <= 0:
            return pd.DataFrame()

        expirations = stock.options
        rows = []

        for expiration_date in expirations:
            if expiration_date > MAX_EXPIRATION:
                continue

            exp_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
            if (exp_date - today).days <= 0:
                continue

            chain = stock.option_chain(expiration_date)

            for option_type, option_frame in [("call", chain.calls), ("put", chain.puts)]:
                normalized = coerce_option_frame(
                    option_frame,
                    underlying_price=underlying_price,
                    expiration_date=expiration_date,
                    option_type=option_type,
                    ticker=ticker,
                    fetched_at=fetched_at,
                )

                normalized["underlying_price_time"] = snapshot["underlying_price_time"]
                normalized["underlying_currency"] = snapshot["underlying_currency"]
                normalized["underlying_market_state"] = snapshot["underlying_market_state"]
                normalized["underlying_day_change_pct"] = snapshot["underlying_day_change_pct"]
                normalized["underlying_price_age_seconds"] = (
                    (fetched_at - snapshot["underlying_price_time"]).total_seconds()
                    if pd.notna(snapshot["underlying_price_time"])
                    else np.nan
                )
                normalized["is_stale_underlying_price"] = np.where(
                    pd.notna(normalized["underlying_price_age_seconds"]),
                    normalized["underlying_price_age_seconds"] > STALE_QUOTE_SECONDS,
                    pd.NA,
                )
                normalized["fetch_status"] = "ok"
                normalized["fetch_error"] = ""

                rows.append(normalized)

        return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()

    except Exception as exc:
        print(f"{ticker} error: {exc}")
        return pd.DataFrame()


# ── LOAD ALL ───────────────────────────────────────────
dfs = []
for ticker in TICKERS:
    print(f"Loading {ticker}")
    ticker_df = fetch_chain(ticker)
    if not ticker_df.empty:
        dfs.append(ticker_df)

if not dfs:
    print("No data fetched.")
    raise SystemExit(0)

df = pd.concat(dfs, ignore_index=True)

# Keep the full enriched dataset so downstream tools can decide their own screens.
df["option_quote_time"] = df["option_quote_time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
df["underlying_price_time"] = pd.to_datetime(df["underlying_price_time"], utc=True, errors="coerce").dt.strftime(
    "%Y-%m-%dT%H:%M:%SZ"
)
df["fetched_at"] = pd.to_datetime(df["fetched_at"], utc=True, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%SZ")

df.to_csv("options_engine_output.csv", index=False)
print("\nSaved: options_engine_output.csv")
