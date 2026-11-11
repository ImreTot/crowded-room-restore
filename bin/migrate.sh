#!/bin/bash

docker compose -f docker/docker-compose.dev.yml run --rm api alembic upgrade head
