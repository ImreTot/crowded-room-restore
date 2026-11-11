#!/bin/bash

docker compose -f docker/docker-compose.dev.yml up --remove-orphans --build
