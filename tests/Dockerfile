FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --all-extras --no-install-project

COPY . .
RUN uv sync --all-extras

CMD ["uv", "run", "python", "/app/tests/src/stream_simulation.py"]
