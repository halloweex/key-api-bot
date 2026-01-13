FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY bot/ ./bot/
COPY core/ ./core/

# Create data directory for SQLite database
RUN mkdir -p /app/data

# Volume for persistent database storage
VOLUME ["/app/data"]

# Run the bot
CMD ["python3", "-m", "bot.main"]
