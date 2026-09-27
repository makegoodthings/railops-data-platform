FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY tests ./tests
RUN pip install --no-cache-dir ".[dev]"

COPY docs ./docs
COPY airflow ./airflow

ENTRYPOINT []
CMD ["python", "-m", "railops.cli", "--help"]
