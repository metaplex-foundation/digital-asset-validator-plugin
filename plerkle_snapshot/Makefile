#colors:
B = \033[1;94m#   BLUE
G = \033[1;92m#   GREEN
Y = \033[1;93m#   YELLOW
R = \033[1;31m#   RED
M = \033[1;95m#   MAGENTA
C=  \033[1;96m#   CYAN
K = \033[K#       ERASE END OF LINE
D = \033[0m#      DEFAULT
A = \007#         BEEP

.PHONY: build push clean download stream

SHELL := /bin/bash
APP = solana-snapshot-etl
REPO_NAME = $(shell basename -s .git $(shell git remote get-url origin))
REGISTRY=ghcr.io/obezsmertnyi
VERSION=$(shell git describe --tags --abbrev=0)
BASEDIR = ./snapshot/
BASEPATH = ${REGISTRY}/${REPO_NAME}/${APP}:${VERSION}

build:
	@echo -e "${M}Starting build for Docker image: ${APP}${D}"
	@echo -e "${C}Using the following settings:${D}"
	@echo -e "${C}- Application: ${APP}${D}"
	@echo -e "${C}- Registry: ${REGISTRY}${D}"
	@echo -e "${C}- Version (tag): ${VERSION}${D}\n"
	@echo -e "${C}Pulling the latest base image: c29r3/solana-snapshot-finder:latest${D}"
	@docker pull c29r3/solana-snapshot-finder:latest || { echo -e "${R}Error: Failed to pull the base image.${D}"; exit 1; }
	@echo -e "${C}Building Docker image: ${APP} with version: ${VERSION}${D}\n"
	@docker build -f Dockerfile . -t ${BASEPATH} || { echo -e "${R}Error: Build failed.${D}"; exit 1; }
	@echo -e "${G}Build completed successfully! Image: ${BASEPATH}${D}"

push:
	@echo -e "${M}Pushing Docker image to the registry: ${BASEPATH}${D}"
	@docker push ${BASEPATH} || { echo -e "${R}Error: Failed to push the image.${D}"; exit 1; }
	@docker tag ${BASEPATH} ${REGISTRY}/${REPO_NAME}/${APP}:latest
	@docker push ${REGISTRY}/${REPO_NAME}/${APP}:latest || { echo -e "${R}Error: Failed to push the 'latest' tag.${D}"; exit 1; }
	@echo -e "${G}Image pushed successfully!${D}"

clean:
	@echo -e "${M}Cleaning up Docker images created for ${APP} version ${VERSION}...${D}"
	@if docker images ${BASEPATH} -q | grep -q '.' ; then \
		echo -e "${Y}Removing Docker image ${BASEPATH}${D}"; \
		docker rmi ${BASEPATH} || { echo -e "${R}Error: Failed to remove the image.${D}"; exit 1; } \
	else \
		echo -e "${R}No Docker image found for ${BASEPATH}${D}"; \
	fi
	@echo -e "${G}Cleanup completed for ${APP} version ${VERSION}!${D}"

download:
	@rm -f ./snapshot/*
	@mkdir -p ./snapshot
	@docker run -it --rm -v $(PWD)/snapshot:/snapshot --user $(id -u):$(id -g) c29r3/solana-snapshot-finder:latest --snapshot_path /snapshot

check-synchronizer:
	@docker exec -it synchronizer sh -c ' \
		while true; do \
			solana_slot=$$(curl -s -X POST -H "Content-Type: application/json" -d '\''{"jsonrpc": "2.0","id": 1,"method": "getSlot","params": [{"commitment": "processed"}]}'\'' $$INGESTER_RPC_HOST | grep -oP "(?<=\"result\":)[0-9]+"); \
			synchronizer_slot=$$(curl -s localhost:$$SYNCHRONIZER_METRICS_PORT/metrics | grep "synchronizer_last_synchronized_slot{name=\"last_synchronized_slot\"}" | awk '\''{print $$2}'\''); \
			difference=$$((solana_slot - synchronizer_slot)); \
			echo "Solana slot: $$solana_slot"; \
			echo "Synchronizer last synchronized slot: $$synchronizer_slot"; \
			echo "Difference: $$difference slots"; \
			if [ "$$difference" -lt 50 ]; then break; fi; \
			sleep 5; \
		done 2>/dev/null'
stream:
	@INGESTER_RPC_HOST=$$(docker exec ingester-new env | grep INGESTER_RPC_HOST | cut -d "=" -f2); \
	docker exec -it synchronizer sh -c ' \
		#SYNCHRONIZER_METRICS_PORT=$$(env | grep SYNCHRONIZER_METRICS_PORT | cut -d "=" -f2); \
		#INGESTER_RPC_HOST=$$(env | grep INGESTER_RPC_HOST | cut -d "=" -f2); \
		echo "\033[1;94mSYNCHRONIZER_METRICS_PORT: $$SYNCHRONIZER_METRICS_PORT\033[0m"; \
		echo "\033[1;94mINGESTER_RPC_HOST: $$INGESTER_RPC_HOST\033[0m"; \
		while true; do \
			# Get current Solana slot \
                        solana_slot=$$(curl -s -X POST -H "Content-Type: application/json" -d '\''{"jsonrpc": "2.0","id": 1,"method": "getSlot","params": [{"commitment": "processed"}]}'\'' $$INGESTER_RPC_HOST | grep -oP "(?<=\"result\":)[0-9]+"); \
			echo "\033[1;92mSolana slot: $$solana_slot\033[0m"; \
			# Get last synchronized slot from synchronizer \
                        synchronizer_slot=$$(curl -s localhost:$$SYNCHRONIZER_METRICS_PORT/metrics | grep "synchronizer_last_synchronized_slot{name=\"last_synchronized_slot\"}" | awk '\''{print $$2}'\''); \
			echo "\033[1;92mSynchronizer last synchronized slot: $$synchronizer_slot\033[0m"; \
			# Calculate the difference between Solana and synchronizer slots \
			difference=$$((solana_slot - synchronizer_slot)); \
			echo "\033[1;93mDifference: $$difference slots\033[0m"; \
			# Check if the difference is below threshold \
			if [ "$$difference" -lt 50 ]; then \
				echo  "\033[1;92mSlot difference is below threshold. Exiting loop.\033[0m"; \
				break; \
			fi; \
			# Wait before next iteration \
			echo "\033[1;95mWaiting for 5 seconds before next check...\033[0m"; \
			sleep 5; \
		done 2>/dev/null'

