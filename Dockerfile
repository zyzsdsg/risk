FROM python:3.11-slim

WORKDIR /app

# Install uv
RUN pip install uv

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (直接用 pip 更可靠)
RUN pip install fastapi uvicorn sqlalchemy psycopg2-binary \
    pydantic pydantic-settings python-dotenv \
    pandas numpy yfinance requests

# Copy source code
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]