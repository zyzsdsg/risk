from datetime import datetime, date
from sqlalchemy import (
    Column,
    Integer,
    String,
    Numeric,
    Date,
    DateTime,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Asset(Base):
    """
    资产信息表 — 静态数据，只需填一次
    例：ticker="ANZ.NZ", name="ANZ Bank NZ", region="NZ"
    """

    __tablename__ = "assets"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String(20), nullable=False, unique=True)
    name = Column(String(200), nullable=False)
    region = Column(String(10), nullable=False)  # "NZ", "AU", "US"
    sector = Column(String(100))  # "Financials"
    asset_type = Column(String(20), default="stock")  # "stock", "etf"
    created_at = Column(DateTime, default=datetime.utcnow)

    # 关系：一只股票对应多条价格记录
    prices = relationship("DailyPrice", back_populates="asset")
    risk_metrics = relationship("RiskMetric", back_populates="asset")
    alerts = relationship("Alert", back_populates="asset")

    def __repr__(self):
        return f"<Asset {self.ticker}>"


class DailyPrice(Base):
    """
    每日价格表 — 每天自动更新
    每只股票每天一条记录
    """

    __tablename__ = "daily_prices"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    price_date = Column(Date, nullable=False)
    open_price = Column(Numeric(12, 4))
    high_price = Column(Numeric(12, 4))
    low_price = Column(Numeric(12, 4))
    close_price = Column(Numeric(12, 4), nullable=False)
    volume = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    # 联合唯一约束：同一只股票同一天只能有一条记录
    __table_args__ = (UniqueConstraint("asset_id", "price_date", name="uq_asset_date"),)

    asset = relationship("Asset", back_populates="prices")

    def __repr__(self):
        return f"<DailyPrice {self.asset_id} {self.price_date}>"


class RiskMetric(Base):
    """
    风险指标表 — 从 daily_prices 计算得出
    这是整个项目最有含金量的表
    """

    __tablename__ = "risk_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    calc_date = Column(Date, nullable=False)
    volatility_30d = Column(Numeric(10, 6))  # 30天年化波动率
    var_95 = Column(Numeric(10, 6))  # 95% 在险价值
    sharpe_30d = Column(Numeric(10, 6))  # 夏普比率
    beta = Column(Numeric(10, 6))  # 相对基准的 beta
    max_drawdown = Column(Numeric(10, 6))  # 最大回撤
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("asset_id", "calc_date", name="uq_risk_asset_date"),
    )

    asset = relationship("Asset", back_populates="risk_metrics")

    def __repr__(self):
        return f"<RiskMetric {self.asset_id} {self.calc_date}>"


class MacroIndicator(Base):
    """
    宏观经济指标表 — 从 FRED API 拉取
    例：GDP, CPI, 利率, 失业率
    """

    __tablename__ = "macro_indicators"

    id = Column(Integer, primary_key=True, autoincrement=True)
    indicator_code = Column(String(50), nullable=False)  # "FEDFUNDS"
    indicator_name = Column(String(200))  # "Federal Funds Rate"
    report_date = Column(Date, nullable=False)
    value = Column(Numeric(20, 6), nullable=False)
    country = Column(String(10), nullable=False)  # "US", "NZ"
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("indicator_code", "report_date", name="uq_macro_code_date"),
    )

    def __repr__(self):
        return f"<MacroIndicator {self.indicator_code} {self.report_date}>"


class Alert(Base):
    """
    预警表 — 当风险指标超过阈值时触发
    例：波动率超过 40% 触发警告
    """

    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    alert_type = Column(String(50), nullable=False)  # "HIGH_VOLATILITY"
    threshold = Column(Numeric(10, 6), nullable=False)
    actual_value = Column(Numeric(10, 6), nullable=False)
    triggered_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Integer, default=1)  # 1=active, 0=resolved

    asset = relationship("Asset", back_populates="alerts")

    def __repr__(self):
        return f"<Alert {self.alert_type} {self.asset_id}>"
