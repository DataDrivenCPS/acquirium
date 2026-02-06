FROM python:3.14-slim

WORKDIR /app

COPY ./scripts /app/
COPY ./benicia-model-100.ttl /app/benicia-model-100.ttl
RUN pip install -r /app/requirements.txt

CMD ["python", "/app/stream_simulator.py","--model","/app/benicia-model-100.ttl","--broker","mosquitto"]
