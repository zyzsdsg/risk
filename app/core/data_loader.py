import pandas as pd
import logging
from sqlalchemy.orm import Session
from app.db.models import DailyPrice, Asset

logger = logging.getLogger(__name__)


def load_prices_for_ticker(db: Session, ticker: str) -> pd.DataFrame:
    """
    Load full price history for a single ticker from the database.
    Returns a DataFrame indexed by date with OHLCV columns.
    """
    rows = (
        db.query(DailyPrice)
        .join(Asset)
        .filter(Asset.ticker == ticker)
        .order_by(DailyPrice.price_date)
        .all()
    )

    if not rows:
        logger.warning(f"No price data found for {ticker}")
        return pd.DataFrame()

    data = [
        {
            "date": row.price_date,
            "open": float(row.open_price or 0),
            "high": float(row.high_price or 0),
            "low": float(row.low_price or 0),
            "close": float(row.close_price),
            "volume": int(row.volume or 0),
            "asset_id": row.asset_id,
        }
        for row in rows
    ]

    df = pd.DataFrame(data)
    df = df.set_index("date")
    df.index = pd.to_datetime(df.index)
    return df


def load_all_tickers(db: Session) -> list[str]:
    """Return a list of all ticker symbols stored in the database."""
    assets = db.query(Asset.ticker).all()
    return [a.ticker for a in assets]


def load_benchmark(db: Session, benchmark_ticker: str = "SPY") -> pd.Series:
    """
    Load daily returns for the benchmark index (default: SPY).
    Used for beta calculation. Returns empty Series if not available.
    """
    df = load_prices_for_ticker(db, benchmark_ticker)
    if df.empty:
        return pd.Series(dtype=float)
    return df["close"].pct_change().dropna()
