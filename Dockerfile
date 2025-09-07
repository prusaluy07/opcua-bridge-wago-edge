FROM python:3.11-slim

WORKDIR /app

# Systemtools installieren (inkl. curl)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

CMD ["python", "bridge.py"]
