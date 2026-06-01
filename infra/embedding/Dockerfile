FROM python:3.12-slim

RUN addgroup --system app && adduser --system --ingroup app app

WORKDIR /app
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    "sentence-transformers>=3.0" \
    torch \
    numpy

COPY infra/embedding/server.py /app/server.py

USER app
ENV PYTHONPATH=/app

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001"]
