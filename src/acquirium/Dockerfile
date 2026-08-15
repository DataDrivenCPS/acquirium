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

# Install the project itself into the already-populated venv.
RUN uv sync --locked --no-dev

# Ray per-app/driver runtime environments clone this venv and pip-install
# the declared packages on top. That path hard-requires `virtualenv`
# importable in the base env (ray's PipProcessor imports it even when only
# cloning) and pip inside the venv — uv-managed venvs ship neither, so
# without this line every pip runtime_env fails to build in the image.
# virtualenv is pinned <21: newer releases drop the clone entry point Ray
# uses.
RUN uv pip install pip "virtualenv<21"

# Copy config last so edits don't invalidate the layers above.
COPY acquirium.docker.toml ./acquirium.toml


# Pre-warm the embedding model so the first request isn't slow.
ENV FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN uv run python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

EXPOSE 8000

CMD ["uv", "run", "acquirium", "server", "--host", "0.0.0.0", "--port", "8000"]
