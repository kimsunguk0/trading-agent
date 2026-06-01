FROM python:3.12-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY core /app/core
COPY brokers /app/brokers
COPY apps /app/apps
COPY infra /app/infra
COPY configs /app/configs
COPY tests /app/tests
RUN pip install --no-cache-dir -e .[dev]

USER app
ENV PYTHONPATH=/app

CMD ["uvicorn", "apps.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
