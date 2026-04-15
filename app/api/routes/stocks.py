from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import queries

router = APIRouter()


@router.get("/stocks")
def get_all_stocks(db: Session = Depends(get_db)):
    """Return list of all tracked stocks"""
    assets = queries.get_all_assets(db)
    return [
        {
            "id": a.id,
            "ticker": a.ticker,
            "name": a.name,
            "region": a.region,
            "sector": a.sector,
            "asset_type": a.asset_type,
        }
        for a in assets
    ]


@router.get("/stocks/{ticker}")
def get_stock(ticker: str, db: Session = Depends(get_db)):
    """Return a single stock by ticker"""
    asset = queries.get_asset_by_ticker(db, ticker.upper())
    if not asset:
        raise HTTPException(status_code=404, detail=f"{ticker} not found")
    return {
        "id": asset.id,
        "ticker": asset.ticker,
        "name": asset.name,
        "region": asset.region,
        "sector": asset.sector,
        "asset_type": asset.asset_type,
    }


@router.get("/stocks/{ticker}/prices")
def get_stock_prices(ticker: str, db: Session = Depends(get_db)):
    """Return full price history for a stock"""
    asset = queries.get_asset_by_ticker(db, ticker.upper())
    if not asset:
        raise HTTPException(status_code=404, detail=f"{ticker} not found")
    prices = queries.get_prices_by_ticker(db, ticker.upper())
    return [
        {
            "date": str(p.price_date),
            "open": float(p.open_price or 0),
            "high": float(p.high_price or 0),
            "low": float(p.low_price or 0),
            "close": float(p.close_price or 0),
            "volume": int(p.volume or 0),
        }
        for p in prices
    ]
