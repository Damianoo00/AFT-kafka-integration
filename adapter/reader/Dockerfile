FROM python:3.12-slim

RUN apt-get update && apt-get install -y build-essential librdkafka-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY reader.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "reader.py"]
