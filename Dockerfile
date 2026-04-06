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
    pip install . && \
    mkdir -p /workspace && \
    cp petaly.ini-template /workspace/petaly.ini && \
    sed -i 's|^pipeline_dir_path=.*|pipeline_dir_path=/workspace/pipelines|' /workspace/petaly.ini && \
    sed -i 's|^connections_file_path=.*|connections_file_path=/workspace/connections.yaml|' /workspace/petaly.ini && \
    sed -i 's|^logs_dir_path=.*|logs_dir_path=/workspace/logs|' /workspace/petaly.ini && \
    sed -i 's|^output_dir_path=.*|output_dir_path=/workspace/output|' /workspace/petaly.ini

ENTRYPOINT ["petaly"]
CMD ["--help"]
