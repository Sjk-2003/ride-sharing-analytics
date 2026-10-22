# SwiftRide Bengaluru live website: small Python image, one web worker
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY stats.py app.py ./
COPY static ./static

EXPOSE 8080
# One worker only: the ride totals live in this one process's memory
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--log-level", "warning"]
