FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

#RUN apt-get update \
#  && apt-get install -y --no-install-recommends docker.io ca-certificates \
#  && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml /app/pyproject.toml
COPY src /app/src
COPY README.md /app/README.md
COPY .python-version /app/.python-version
COPY ontologies /app/ontologies
RUN uv sync

ENV FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN uv run python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "acquirium.Server.app:app", "--host", "0.0.0.0", "--port", "8000"]
