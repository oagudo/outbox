#!/bin/bash

# Run in daemon mode
docker compose up -d

echo "Waiting for containers to become healthy..."

# Wait for all containers to be healthy
while true; do
    unhealthy=$(docker inspect --format='{{.Name}} {{.State.Health.Status}}' $(docker compose ps -q) | grep -v healthy || true)
    if [ -z "$unhealthy" ]; then
        echo "All containers are healthy."
        break
    fi
    sleep 1
done
