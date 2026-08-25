FROM python:3.14-slim

# Set working directory
WORKDIR /app

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

# Create non-root user and data directory
RUN groupadd -r appuser && useradd -r -g appuser -d /app appuser \
    && mkdir -p /app/data && chown -R appuser:appuser /app

# Volume for persistent database storage
VOLUME ["/app/data"]

USER appuser

# Run the bot
CMD ["python3", "-m", "bot.main"]
