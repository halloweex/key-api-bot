#!/bin/bash
set -e

echo "🚀 Deploying KeyCRM Bot to EC2..."

# Pull latest code
git pull origin main

# Pull latest Docker image
docker-compose pull

# Restart containers
docker-compose down
docker-compose up -d

# Clean up old images
docker image prune -f

echo "✅ Deployment complete!"
docker-compose ps
