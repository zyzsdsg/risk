from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db import queries

router = APIRouter()


def safe_float(value):
    """Convert to float, replace nan/inf with 0"""
    try:
        f = float(value or 0)
        if f != f:  # nan check
            return 0.0
        return f
    except:
        return 0.0


@router.get("/risk")
def get_all_risk(db: Session = Depends(get_db)):
    metrics = queries.get_all_latest_risk(db)
    return [
        {
            "asset_id": m.asset_id,
            "ticker": m.asset.ticker,
            "name": m.asset.name,
            "calc_date": str(m.calc_date),
            "volatility_30d": safe_float(m.volatility_30d),
            "var_95": safe_float(m.var_95),
            "sharpe_30d": safe_float(m.sharpe_30d),
            "beta": safe_float(m.beta),
            "max_drawdown": safe_float(m.max_drawdown),
        }
        for m in metrics
    ]


@router.get("/risk/{ticker}")
def get_risk_by_ticker(ticker: str, db: Session = Depends(get_db)):
    metric = queries.get_latest_risk_by_ticker(db, ticker.upper())
    if not metric:
        raise HTTPException(status_code=404, detail=f"No risk data for {ticker}")
    return {
        "asset_id": metric.asset_id,
        "ticker": metric.asset.ticker,
        "name": metric.asset.name,
        "calc_date": str(metric.calc_date),
        "volatility_30d": safe_float(metric.volatility_30d),
        "var_95": safe_float(metric.var_95),
        "sharpe_30d": safe_float(metric.sharpe_30d),
        "beta": safe_float(metric.beta),
        "max_drawdown": safe_float(metric.max_drawdown),
    }
