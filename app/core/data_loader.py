from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # App
    app_env: str = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # Database
    database_url: str

    # Cache TTL (seconds)
    cache_ttl_seconds: int = 3600  # 1 hour

    # yfinance
    yfinance_period: str = "1y"  # 拉多长历史数据
    yfinance_interval: str = "1d"  # 日频数据

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()
