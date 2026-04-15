from sqlalchemy.orm import Session
from app.db.models import Asset, DailyPrice, RiskMetric, MacroIndicator, Alert
from datetime import date

# ══════════════════════════════
# Assets
# ══════════════════════════════


def get_asset_by_ticker(db: Session, ticker: str) -> Asset | None:
    return db.query(Asset).filter(Asset.ticker == ticker).first()


def get_all_assets(db: Session) -> list[Asset]:
    return db.query(Asset).all()


def create_asset(
    db: Session,
    ticker: str,
    name: str,
    region: str,
    sector: str = None,
    asset_type: str = "stock",
) -> Asset:
    asset = Asset(
        ticker=ticker, name=name, region=region, sector=sector, asset_type=asset_type
    )
    db.add(asset)
    db.commit()
    db.refresh(asset)
    return asset


# ══════════════════════════════
# Daily Prices
# ══════════════════════════════


def upsert_daily_price(
    db: Session,
    asset_id: int,
    price_date: date,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: int,
) -> DailyPrice:
    existing = (
        db.query(DailyPrice)
        .filter(DailyPrice.asset_id == asset_id, DailyPrice.price_date == price_date)
        .first()
    )

    if existing:
        existing.open_price = open_price
        existing.high_price = high_price
        existing.low_price = low_price
        existing.close_price = close_price
        existing.volume = volume
        db.commit()
        db.refresh(existing)
        return existing
    else:
        price = DailyPrice(
            asset_id=asset_id,
            price_date=price_date,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
            volume=volume,
        )
        db.add(price)
        db.commit()
        db.refresh(price)
        return price


def get_prices_by_ticker(
    db: Session, ticker: str, start_date: date = None
) -> list[DailyPrice]:
    query = db.query(DailyPrice).join(Asset).filter(Asset.ticker == ticker)
    if start_date:
        query = query.filter(DailyPrice.price_date >= start_date)
    return query.order_by(DailyPrice.price_date).all()


# ══════════════════════════════
# Risk Metrics
# ══════════════════════════════


def upsert_risk_metric(
    db: Session,
    asset_id: int,
    calc_date: date,
    volatility_30d: float,
    var_95: float,
    sharpe_30d: float,
    beta: float,
    max_drawdown: float,
) -> RiskMetric:
    existing = (
        db.query(RiskMetric)
        .filter(RiskMetric.asset_id == asset_id, RiskMetric.calc_date == calc_date)
        .first()
    )

    if existing:
        existing.volatility_30d = volatility_30d
        existing.var_95 = var_95
        existing.sharpe_30d = sharpe_30d
        existing.beta = beta
        existing.max_drawdown = max_drawdown
        db.commit()
        db.refresh(existing)
        return existing
    else:
        metric = RiskMetric(
            asset_id=asset_id,
            calc_date=calc_date,
            volatility_30d=volatility_30d,
            var_95=var_95,
            sharpe_30d=sharpe_30d,
            beta=beta,
            max_drawdown=max_drawdown,
        )
        db.add(metric)
        db.commit()
        db.refresh(metric)
        return metric


def get_latest_risk_by_ticker(db: Session, ticker: str) -> RiskMetric | None:
    return (
        db.query(RiskMetric)
        .join(Asset)
        .filter(Asset.ticker == ticker)
        .order_by(RiskMetric.calc_date.desc())
        .first()
    )


def get_all_latest_risk(db: Session) -> list[RiskMetric]:
    from sqlalchemy import func

    subquery = (
        db.query(RiskMetric.asset_id, func.max(RiskMetric.calc_date).label("max_date"))
        .group_by(RiskMetric.asset_id)
        .subquery()
    )

    return (
        db.query(RiskMetric)
        .join(
            subquery,
            (RiskMetric.asset_id == subquery.c.asset_id)
            & (RiskMetric.calc_date == subquery.c.max_date),
        )
        .all()
    )


# ══════════════════════════════
# Macro Indicators
# ══════════════════════════════


def upsert_macro_indicator(
    db: Session,
    indicator_code: str,
    indicator_name: str,
    report_date: date,
    value: float,
    country: str,
) -> MacroIndicator:
    existing = (
        db.query(MacroIndicator)
        .filter(
            MacroIndicator.indicator_code == indicator_code,
            MacroIndicator.report_date == report_date,
        )
        .first()
    )

    if existing:
        existing.value = value
        existing.indicator_name = indicator_name
        db.commit()
        db.refresh(existing)
        return existing
    else:
        macro = MacroIndicator(
            indicator_code=indicator_code,
            indicator_name=indicator_name,
            report_date=report_date,
            value=value,
            country=country,
        )
        db.add(macro)
        db.commit()
        db.refresh(macro)
        return macro


def get_macro_by_code(db: Session, indicator_code: str) -> list[MacroIndicator]:
    return (
        db.query(MacroIndicator)
        .filter(MacroIndicator.indicator_code == indicator_code)
        .order_by(MacroIndicator.report_date)
        .all()
    )


# ══════════════════════════════
# Alerts
# ══════════════════════════════


def create_alert(
    db: Session, asset_id: int, alert_type: str, threshold: float, actual_value: float
) -> Alert:
    alert = Alert(
        asset_id=asset_id,
        alert_type=alert_type,
        threshold=threshold,
        actual_value=actual_value,
        is_active=1,
    )
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert


def get_active_alerts(db: Session) -> list[Alert]:
    return db.query(Alert).filter(Alert.is_active == 1).all()


def resolve_alert(db: Session, alert_id: int) -> Alert | None:
    alert = db.query(Alert).filter(Alert.id == alert_id).first()
    if alert:
        alert.is_active = 0
        db.commit()
        db.refresh(alert)
    return alert
