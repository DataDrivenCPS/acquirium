# Python version is parameterized so the server image can be built against
# any supported interpreter (3.11–3.14). Defaults to 3.12.
ARG PYTHON_VERSION=3.12
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim

WORKDIR /app

# Copy dependency manifest first — this layer is only invalidated when
# pyproject.toml changes, so source edits don't trigger a full dep reinstall.
# Deliberately NOT copying .python-version: it pins 3.12 and would override
# the base image's interpreter, defeating the PYTHON_VERSION build arg.
COPY pyproject.toml uv.lock README.md ./

# Install dependencies but not the project itself.
RUN uv sync --locked --no-dev --no-install-project

# Now copy source. Bundled ontologies live under src/acquirium/_ontologies/
# and are installed with the package — no separate ontologies/ copy needed.
COPY src ./src

# Install the project itself into the already-populated venv. Per-driver/app
# environments are overlay venvs the server materializes itself under the
# data dir (env_spec.ensure_env) — Ray's pip runtime envs are not used, so
# no pip/virtualenv special-casing is needed here.
RUN uv sync --locked --no-dev

# Copy config last so edits don't invalidate the layers above.
COPY acquirium.docker.toml ./acquirium.toml


# Pre-warm the embedding model so the first request isn't slow.
ENV FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN uv run python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

EXPOSE 8000

CMD ["uv", "run", "acquirium", "server", "--host", "0.0.0.0", "--port", "8000"]
