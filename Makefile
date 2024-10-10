.PHONY: build start dev stop test mocks lint

SHELL := /bin/bash

ifneq (,$(wildcard .env))
	include .env
	export $(shell sed 's/=.*//' .env)
endif

export IMAGE_NAME=solana-snapshot-etl

build:
	@docker build -f Dockerfile . -t ${IMAGE_NAME}

stream:
	for f in $(shell ls ${SNAPSHOTDIR}); do echo $$(realpath $${f}) && docker run --env-file .env -p 3000:3000 --rm -it --mount type=bind,source=$$(realpath $${f}),target=$$(realpath $${f}),readonly --mount type=bind,source=$$(pwd)/etl-config.json,target=/app/etl-config.json,readonly ${IMAGE_NAME} $$(realpath $${f}) --geyser=./etl-config.json && date; done
