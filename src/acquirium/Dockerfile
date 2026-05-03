FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Copy dependency manifest first — this layer is only invalidated when
# pyproject.toml changes, so source edits don't trigger a full dep reinstall.
COPY pyproject.toml uv.lock README.md .python-version ./

# Install dependencies but not the project itself.
RUN uv sync --locked --no-dev --no-install-project

# Now copy source and ontologies; changes here only re-run the steps below.
COPY src ./src
COPY ontologies ./ontologies

# Install the project itself into the already-populated venv.
RUN uv sync --locked --no-dev

# Pre-warm the embedding model so the first request isn't slow.
ENV FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN uv run python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "acquirium.Server.app:app", "--host", "0.0.0.0", "--port", "8000"]
