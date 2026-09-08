# Base images are pinned by digest, not by tag. `python:3.14-slim` is a moving
# target: it is rebuilt on every CPython patch and on every base-OS update, so
# building the same commit twice can produce two different runtimes. That is
# the same exposure `requirements.lock` closed one layer up — there pip decided
# what shipped, here Docker Hub does.
#
# The digest is the multi-arch **index**, so per-platform resolution still
# works; the tag is kept in the comment because a bare sha256 tells a reader
# nothing.
#
# COST, ACCEPTED: base-image security patches no longer arrive on a rebuild.
# Refresh deliberately, and read what moved:
#
#   docker buildx imagetools inspect python:3.14-slim | grep '^Digest:'
#
# Only the BUILD-time bases are pinned. The service images in
# docker-compose.yml (nginx, postgres, meilisearch) are deliberately left on
# tags: pinning those by digest makes the next `up -d` recreate the containers,
# and one of them is now the system of record for the Postgres mirror.
FROM python:3.14-slim@sha256:83ff1d245a3d57d04152252d3ef9cb361494d0b3395abd65a5ebe91c401c8e83

# Set working directory
WORKDIR /app

# Fonts, for the same reason the web image has them: `core/` travels into this
# image, renderers included, so anything this container draws should come out
# in the brand's hand rather than not at all. DejaVu is not decoration here —
# it is the fallback face for the two arrow glyphs the brand fonts do not
# carry, and without it those become empty boxes with nothing said. ~2 MB.
#
# Nothing under `bot/` draws today; this makes the capability whole instead of
# half-present, and keeps the two images from disagreeing about what they can
# render.
RUN apt-get update && apt-get install -y --no-install-recommends \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies
# The lock, not the intent file: `requirements.txt` has floors and no
# ceilings, so building from it lets pip decide what production runs.
# It crossed a major starlette boundary that way. See requirements.lock.
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock

# Copy version file and application code
COPY VERSION ./
COPY bot/ ./bot/
COPY core/ ./core/

# The brand faces and the marks the renderers in core/ look for.
COPY assets/ ./assets/

# Create non-root user and data directory
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser \
    && mkdir -p /app/data && chown -R appuser:appuser /app

# Volume for persistent database storage
VOLUME ["/app/data"]

USER appuser

# Run the bot
CMD ["python3", "-m", "bot.main"]
