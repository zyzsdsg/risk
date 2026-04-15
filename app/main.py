import logging
from app.config import settings
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import health, stocks, risk

####Set up logging
log_level = logging.DEBUG if settings.debug else logging.INFO
logging.basicConfig(level=log_level)


app = FastAPI(title="Financial Risk Metrics API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://aifintech.dev",
        "https://www.aifintech.dev",
        "https://aifintech-portfolio.vercel.app",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(health.router, tags=["Health"])
app.include_router(stocks.router, tags=["Stocks"])
app.include_router(risk.router, tags=["Risk"])
