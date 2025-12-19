FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml /app/pyproject.toml
COPY tests /app/tests
COPY README.md /app/README.md
COPY .python-version /app/.python-version
RUN pip install -r /app/tests/src/requirements.txt

CMD ["python", "/app/tests/src/stream_simulation.py"]
