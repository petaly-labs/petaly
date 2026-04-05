FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install runtime tools commonly needed for ETL workflows and connector debugging.
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml README.md LICENSE.md ./
COPY src ./src
COPY docs ./docs
COPY petaly.ini-template ./

RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install .

ENTRYPOINT ["petaly"]
CMD ["--help"]
