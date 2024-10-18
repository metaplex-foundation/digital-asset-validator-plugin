#colors:
B = \033[1;94m#   BLUE
G = \033[1;92m#   GREEN
Y = \033[1;93m#   YELLOW
R = \033[1;31m#   RED
M = \033[1;95m#   MAGENTA
C = \033[1;96m#   CYAN
K = \033[K#       ERASE END OF LINE
D = \033[0m#      DEFAULT
A = \007#         BEEP

.PHONY: build push clean download stream

SHELL := /bin/bash
#APP = solana-snapshot-etl
#REGISTRY = ghcr.io/metaplex-foundation 
REPO_NAME = $(shell basename -s .git $(shell git remote get-url origin))
VERSION = $(shell git describe --tags --abbrev=0)
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
		echo -e "${C}Removing Docker image ${BASEPATH}${D}"; \
		docker rmi ${BASEPATH} || { echo -e "${R}Error: Failed to remove the image.${D}"; exit 1; } \
	else \
		echo -e "${R}No Docker image found for ${BASEPATH}${D}"; \
	fi
	@echo -e "${G}Cleanup completed for ${APP} version ${VERSION}!${D}"

download:
	@rm -f ./snapshot/*
	@mkdir -p ./snapshot
	@docker run -it --rm -v $(PWD)/snapshot:/snapshot --user $(id -u):$(id -g) c29r3/solana-snapshot-finder:latest --snapshot_path /snapshot

stream:
	@docker run -p 5000:5000 --rm -v ${BASEDIR}:/snapshots -e TCP_PORT=5000 -e TCP_MIN_SUBSCRIBERS=1 ${BASEPATH}
