FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY tests/src/requirements.txt /app/tests/src/requirements.txt
RUN uv pip install --system -r /app/tests/src/requirements.txt

COPY tests /app/tests

CMD ["python", "/app/tests/src/stream_simulation.py"]
