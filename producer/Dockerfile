# Small Python base image keeps memory and disk use low on t3.small
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# -u = print logs immediately (so "kubectl logs" shows them)
CMD ["python", "-u", "producer.py"]
