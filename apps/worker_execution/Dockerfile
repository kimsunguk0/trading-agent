FROM python:3.12-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY core /app/core
COPY brokers /app/brokers
COPY apps /app/apps
RUN pip install --no-cache-dir -e .[dev]

USER app
ENV PYTHONPATH=/app
CMD ["python", "-m", "apps.worker_execution.__main__"]
