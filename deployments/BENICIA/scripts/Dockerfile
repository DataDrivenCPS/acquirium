FROM python:3.14-slim

WORKDIR /app

COPY deployments/BENICIA/scripts /app/
RUN pip install -r /app/tests/src/requirements.txt

CMD ["python", "/app/tests/src/stream_simulator.py"]
