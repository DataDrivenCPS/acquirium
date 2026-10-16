# syntax=docker/dockerfile:1
# Python version is parameterized so the server image can be built against
# any supported interpreter (3.11–3.14). Defaults to 3.12.
ARG PYTHON_VERSION=3.12
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim

# Building the required native dependency from the pinned source revision.
COPY --from=rust:1.99-bookworm /usr/local/cargo /usr/local/cargo
COPY --from=rust:1.99-bookworm /usr/local/rustup /usr/local/rustup
ENV PATH="/usr/local/cargo/bin:${PATH}" RUSTUP_HOME=/usr/local/rustup
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake git openssh-client ca-certificates && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /root/.ssh && ssh-keyscan github.com >> /root/.ssh/known_hosts

WORKDIR /app

# Copy dependency manifest first — this layer is only invalidated when
# pyproject.toml changes, so source edits don't trigger a full dep reinstall.
# Deliberately NOT copying .python-version: it pins 3.12 and would override
# the base image's interpreter, defeating the PYTHON_VERSION build arg.
COPY pyproject.toml uv.lock README.md ./

# Install dependencies but not the project itself.
RUN --mount=type=ssh uv sync --locked --extra mqtt --no-dev --no-install-project

# Now copy source. Bundled ontologies live under src/acquirium/_ontologies/
# and are installed with the package — no separate ontologies/ copy needed.
COPY src ./src

# Install the project itself into the already-populated venv.
RUN --mount=type=ssh uv sync --locked --extra mqtt --no-dev

# Copy config last so edits don't invalidate the layers above.
COPY acquirium.docker.toml ./acquirium.toml


# Pre-warm the embedding model so the first request isn't slow.
ENV FASTEMBED_CACHE_PATH=/app/.fastembed_cache
RUN uv run python -c "from acquirium.TextMatch.embedding_matcher import load_embedding_model; load_embedding_model()"

EXPOSE 8000

CMD ["uv", "run", "acquirium", "server", "--host", "0.0.0.0", "--port", "8000"]
