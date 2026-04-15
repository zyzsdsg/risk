import pandas as pd
import numpy as np
import logging
from sqlalchemy.orm import Session
from app.db import queries
from app.core.data_loader import (
    load_prices_for_ticker,
    load_all_tickers,
    load_benchmark,
)

logger = logging.getLogger(__name__)

TRADING_DAYS = 252  # annualisation factor
WINDOW = 30  # rolling window in days
RISK_FREE = 0.05  # annualised risk-free rate (approx. RBNZ OCR)


# ══════════════════════════════
# Individual metric functions
# ══════════════════════════════


def calc_volatility(returns: pd.Series, window: int = WINDOW) -> pd.Series:
    """
    Rolling annualised volatility.
    Formula: rolling_std(window) * sqrt(252)
    """
    return returns.rolling(window).std() * np.sqrt(TRADING_DAYS)


def calc_var_95(returns: pd.Series, window: int = WINDOW) -> pd.Series:
    """
    Historical simulation Value at Risk at 95% confidence.
    Formula: 5th percentile of returns over rolling window.
    A value of -0.02 means the worst expected daily loss is 2%.
    """
    return returns.rolling(window).quantile(0.05)


def calc_sharpe(returns: pd.Series, window: int = WINDOW) -> pd.Series:
    """
    Rolling Sharpe ratio.
    Formula: (annualised excess return) / (annualised volatility)
    """
    daily_rf = RISK_FREE / TRADING_DAYS
    excess = returns - daily_rf
    mean = excess.rolling(window).mean() * TRADING_DAYS
    std = returns.rolling(window).std() * np.sqrt(TRADING_DAYS)
    return mean / std.replace(0, np.nan)


def calc_beta(
    returns: pd.Series, benchmark: pd.Series, window: int = WINDOW
) -> pd.Series:
    """
    Rolling beta relative to a benchmark index.
    Formula: cov(stock, benchmark) / var(benchmark)
    Measures systematic risk exposure.
    """
    aligned = pd.concat([returns, benchmark], axis=1).dropna()
    aligned.columns = ["stock", "bench"]

    betas = []
    for i in range(len(aligned)):
        if i < window - 1:
            betas.append(np.nan)
        else:
            chunk = aligned.iloc[i - window + 1 : i + 1]
            cov_matrix = np.cov(chunk["stock"], chunk["bench"])
            bench_var = np.var(chunk["bench"])
            beta = cov_matrix[0][1] / bench_var if bench_var != 0 else np.nan
            betas.append(beta)

    return pd.Series(betas, index=aligned.index)


def calc_max_drawdown(prices: pd.Series, window: int = WINDOW) -> pd.Series:
    """
    Rolling maximum drawdown over the given window.
    Formula: (current_price - rolling_max) / rolling_max
    Negative values indicate percentage decline from the peak.
    """
    rolling_max = prices.rolling(window).max()
    return (prices - rolling_max) / rolling_max


# ══════════════════════════════
# Main calculation pipeline
# ══════════════════════════════


def calculate_risk_for_ticker(db: Session, ticker: str, benchmark: pd.Series = None):
    """
    Calculate all risk metrics for a single ticker and persist to database.
    Skips tickers with fewer than WINDOW days of price data.
    """
    logger.info(f"Calculating risk metrics for {ticker}...")

    df = load_prices_for_ticker(db, ticker)
    if df.empty or len(df) < WINDOW:
        logger.warning(f"Insufficient data for {ticker}, skipping")
        return

    asset_id = int(df["asset_id"].iloc[0])
    returns = df["close"].pct_change().dropna()

    vol = calc_volatility(returns)
    var = calc_var_95(returns)
    sharpe = calc_sharpe(returns)
    drawdown = calc_max_drawdown(df["close"])

    beta_series = (
        calc_beta(returns, benchmark)
        if benchmark is not None and not benchmark.empty
        else pd.Series(np.nan, index=returns.index)
    )

    # Align all metric series into a single DataFrame
    metrics = pd.DataFrame(
        {
            "volatility_30d": vol,
            "var_95": var,
            "sharpe_30d": sharpe,
            "beta": beta_series,
            "max_drawdown": drawdown,
        }
    ).dropna(how="all")

    count = 0
    for calc_date, row in metrics.iterrows():
        if pd.isna(row["volatility_30d"]):
            continue
        queries.upsert_risk_metric(
            db,
            asset_id=asset_id,
            calc_date=calc_date.date(),
            volatility_30d=float(row["volatility_30d"] or 0),
            var_95=float(row["var_95"] or 0),
            sharpe_30d=float(row["sharpe_30d"] or 0),
            beta=float(row["beta"] or 0),
            max_drawdown=float(row["max_drawdown"] or 0),
        )
        count += 1

    logger.info(f"{ticker} complete — {count} rows upserted")


def run_risk_calculation():
    """
    Entry point: calculate risk metrics for all tickers in the database.
    Loads benchmark returns once and reuses across all tickers.
    """
    from app.db.session import SessionLocal

    db = SessionLocal()
    try:
        tickers = load_all_tickers(db)
        benchmark = load_benchmark(db)
        logger.info(f"Starting risk calculation for {len(tickers)} tickers")

        for ticker in tickers:
            try:
                calculate_risk_for_ticker(db, ticker, benchmark)
            except Exception as e:
                logger.error(f"Risk calculation failed for {ticker}: {e}")
                continue

        logger.info("Risk calculation complete")
    finally:
        db.close()


if __name__ == "__main__":
    import logging

    logging.basicConfig(level=logging.INFO)
    run_risk_calculation()
