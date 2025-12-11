FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       ca-certificates \
       pkg-config \
       librdkafka-dev \
       libsasl2-dev \
       libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

ENV FLASK_ENV=production \
    PYTHONUNBUFFERED=1 \
    KAFKA_BOOTSTRAP=broker:29092

CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "app:app"]
