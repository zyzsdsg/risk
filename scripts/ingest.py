import yfinance as yf
import pandas as pd
import logging
from datetime import date, timedelta
from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.db import queries
import argparse

logger = logging.getLogger(__name__)

# ══════════════════════════════
# 股票列表
# ══════════════════════════════

NZX_TICKERS = [
    "ANZ.NZ",
    "CEN.NZ",
    "FPH.NZ",
    "MEL.NZ",
    "AIA.NZ",
    "SPK.NZ",
    "WBC.NZ",
    "MFT.NZ",
    "EBO.NZ",
    "ARG.NZ",
    "PCT.NZ",
    "SKC.NZ",
    "IFT.NZ",
    "GTK.NZ",
    "VHP.NZ",
    "ATM.NZ",
    "HGH.NZ",
    "NZX.NZ",
    "SUM.NZ",
    "CVT.NZ",
]

ASX_TICKERS = [
    "BHP.AX",
    "CBA.AX",
    "ANZ.AX",
    "WBC.AX",
    "NAB.AX",
    "CSL.AX",
    "WES.AX",
    "MQG.AX",
    "TLS.AX",
    "RIO.AX",
    "FMG.AX",
    "WOW.AX",
    "GMG.AX",
    "REA.AX",
    "TCL.AX",
    "ALL.AX",
    "COL.AX",
    "AMC.AX",
    "QBE.AX",
    "SUN.AX",
    "ORG.AX",
    "AGL.AX",
    "MPL.AX",
    "CPU.AX",
    "ASX.AX",
    "MIN.AX",
    "NXT.AX",
    "JHX.AX",
    "SGP.AX",
    "IAG.AX",
]

# ticker → (name, region, sector)
TICKER_INFO = {
    "ANZ.NZ": ("ANZ Bank NZ", "NZ", "Financials"),
    "CEN.NZ": ("Contact Energy", "NZ", "Utilities"),
    "FPH.NZ": ("Fisher & Paykel", "NZ", "Healthcare"),
    "MEL.NZ": ("Meridian Energy", "NZ", "Utilities"),
    "AIA.NZ": ("Auckland Airport", "NZ", "Industrials"),
    "SPK.NZ": ("Spark NZ", "NZ", "Telecom"),
    "WBC.NZ": ("Westpac NZ", "NZ", "Financials"),
    "MFT.NZ": ("Mainfreight", "NZ", "Industrials"),
    "EBO.NZ": ("EBOS Group", "NZ", "Healthcare"),
    "ARG.NZ": ("Argosy Property", "NZ", "Real Estate"),
    "PCT.NZ": ("Precinct Properties", "NZ", "Real Estate"),
    "SKC.NZ": ("SkyCity", "NZ", "Consumer"),
    "IFT.NZ": ("Infratil", "NZ", "Industrials"),
    "GTK.NZ": ("Gentrack", "NZ", "Technology"),
    "VHP.NZ": ("Vital Healthcare", "NZ", "Real Estate"),
    "ATM.NZ": ("a2 Milk", "NZ", "Consumer"),
    "HGH.NZ": ("Heartland Group", "NZ", "Financials"),
    "NZX.NZ": ("NZX Limited", "NZ", "Financials"),
    "SUM.NZ": ("Summerset Group", "NZ", "Healthcare"),
    "CVT.NZ": ("Comvita", "NZ", "Consumer"),
    "BHP.AX": ("BHP Group", "AU", "Materials"),
    "CBA.AX": ("Commonwealth Bank", "AU", "Financials"),
    "ANZ.AX": ("ANZ Bank AU", "AU", "Financials"),
    "WBC.AX": ("Westpac AU", "AU", "Financials"),
    "NAB.AX": ("NAB", "AU", "Financials"),
    "CSL.AX": ("CSL Limited", "AU", "Healthcare"),
    "WES.AX": ("Wesfarmers", "AU", "Consumer"),
    "MQG.AX": ("Macquarie Group", "AU", "Financials"),
    "TLS.AX": ("Telstra", "AU", "Telecom"),
    "RIO.AX": ("Rio Tinto", "AU", "Materials"),
    "FMG.AX": ("Fortescue", "AU", "Materials"),
    "WOW.AX": ("Woolworths", "AU", "Consumer"),
    "GMG.AX": ("Goodman Group", "AU", "Real Estate"),
    "REA.AX": ("REA Group", "AU", "Technology"),
    "TCL.AX": ("Transurban", "AU", "Industrials"),
    "ALL.AX": ("Aristocrat", "AU", "Consumer"),
    "COL.AX": ("Coles Group", "AU", "Consumer"),
    "AMC.AX": ("Amcor", "AU", "Materials"),
    "QBE.AX": ("QBE Insurance", "AU", "Financials"),
    "SUN.AX": ("Suncorp", "AU", "Financials"),
    "ORG.AX": ("Origin Energy", "AU", "Energy"),
    "AGL.AX": ("AGL Energy", "AU", "Energy"),
    "MPL.AX": ("Medibank", "AU", "Healthcare"),
    "CPU.AX": ("Computershare", "AU", "Technology"),
    "ASX.AX": ("ASX Limited", "AU", "Financials"),
    "MIN.AX": ("Mineral Resources", "AU", "Materials"),
    "NXT.AX": ("NEXTDC", "AU", "Technology"),
    "JHX.AX": ("James Hardie", "AU", "Materials"),
    "SGP.AX": ("Stockland", "AU", "Real Estate"),
    "IAG.AX": ("Insurance Australia", "AU", "Financials"),
}

# ══════════════════════════════
# 核心函数
# ══════════════════════════════


def fetch_prices(ticker: str, period: str = "2y") -> pd.DataFrame:
    """用 yfinance 拉价格数据，返回清洗后的 DataFrame"""
    try:
        df = yf.download(ticker, period=period, interval="1d", progress=False)
        if df.empty:
            logger.warning(f"No data returned for {ticker}")
            return pd.DataFrame()

        # 处理多级列名（yfinance 有时返回 MultiIndex）
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.rename(
            columns={
                "Open": "open_price",
                "High": "high_price",
                "Low": "low_price",
                "Close": "close_price",
                "Volume": "volume",
            }
        )

        # 只保留需要的列
        df = df[
            ["open_price", "high_price", "low_price", "close_price", "volume"]
        ].dropna()
        df.index = pd.to_datetime(df.index).date
        return df

    except Exception as e:
        logger.error(f"Failed to fetch {ticker}: {e}")
        return pd.DataFrame()


def ensure_asset(db: Session, ticker: str) -> int:
    """确保 assets 表里有这只股票，返回 asset_id"""
    asset = queries.get_asset_by_ticker(db, ticker)
    if not asset:
        info = TICKER_INFO.get(ticker, (ticker, "Unknown", "Unknown"))
        asset = queries.create_asset(
            db,
            ticker=ticker,
            name=info[0],
            region=info[1],
            sector=info[2],
        )
        logger.info(f"Created asset: {ticker}")
    return asset.id


def ingest_ticker(db: Session, ticker: str, period: str = "2y"):
    """拉一只股票的数据并存入数据库"""
    logger.info(f"Ingesting {ticker}...")

    asset_id = ensure_asset(db, ticker)
    df = fetch_prices(ticker, period=period)

    if df.empty:
        logger.warning(f"Skipping {ticker} — no data")
        return

    count = 0
    for price_date, row in df.iterrows():
        queries.upsert_daily_price(
            db,
            asset_id=asset_id,
            price_date=price_date,
            open_price=float(row["open_price"]),
            high_price=float(row["high_price"]),
            low_price=float(row["low_price"]),
            close_price=float(row["close_price"]),
            volume=int(row["volume"]) if row["volume"] else 0,
        )
        count += 1

    logger.info(f"{ticker} done — {count} rows upserted")


# ══════════════════════════════
# 入口
# ══════════════════════════════


def run_full_ingest():
    """第一次运行：拉2年完整历史数据"""
    db = SessionLocal()
    try:
        all_tickers = NZX_TICKERS + ASX_TICKERS
        logger.info(f"Starting full ingest for {len(all_tickers)} tickers")
        for ticker in all_tickers:
            ingest_ticker(db, ticker, period="2y")
        logger.info("Full ingest complete")
    finally:
        db.close()


def run_daily_update():
    """每天运行：只拉最近5天数据（防止漏数据）"""
    db = SessionLocal()
    try:
        all_tickers = NZX_TICKERS + ASX_TICKERS
        logger.info(f"Starting daily update for {len(all_tickers)} tickers")
        for ticker in all_tickers:
            ingest_ticker(db, ticker, period="5d")
        logger.info("Daily update complete")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest financial data")
    parser.add_argument(
        "--mode",
        choices=["full", "daily"],
        default="full",
        help="full = 2yr history, daily = last 5 days",
    )
    args = parser.parse_args()

    if args.mode == "full":
        run_full_ingest()
    else:
        run_daily_update()
