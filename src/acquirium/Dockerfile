# Python version is parameterized so the server image can be built against
# any supported interpreter (3.11–3.14). Defaults to 3.12.
ARG PYTHON_VERSION=3.12
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim

WORKDIR /app

# Cache the locked dependencies separately from the application source.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --extra mqtt --no-dev --no-install-project

COPY README.md LICENSE ./
COPY src ./src
RUN uv sync --locked --extra mqtt --no-dev --no-editable

# Pre-warm the embedding model so the first request isn't slow.
ENV PATH="/app/.venv/bin:${PATH}" \
    FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN python -c "from acquirium.TextMatch.embedding_matcher import load_embedding_model; load_embedding_model()"

COPY acquirium.docker.toml ./acquirium.toml

EXPOSE 8000

CMD ["acquirium", "server", "--host", "0.0.0.0", "--port", "8000"]
