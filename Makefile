.PHONY: build start dev stop test mocks lint

SHELL := /bin/bash

ifneq (,$(wildcard .env))
	include .env
	export $(shell sed 's/=.*//' .env)
endif

export IMAGE_NAME=solana-snapshot-etl

build:
	@docker build -f Dockerfile . -t ${IMAGE_NAME}

# NOTE: make sure that Redis is reachable from the container.
# If Redis is running locally, it might be feasible to run the container
# with `--net=host` option specified.
stream:
	@export SNAPSHOT_MOUNT="$$(realpath $$SNAPSHOTDIR)" && echo $$SNAPSHOT_MOUNT && docker run --env-file .env --rm -it --mount type=bind,source=$$SNAPSHOT_MOUNT,target=/app/snapshot,ro $$IMAGE_NAME
