#!/bin/bash

set -eo pipefail

TIMESTAMP_FORMAT='+%Y-%m-%d %H:%M:%S'
# This env variable is re-set here, since we expect the snapshots to be mounted at
# /app/snapshot.
SNAPSHOTDIR=/app/snapshot/*.tar.zst

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m'

convert_time() {
    local total_seconds=$1
    local hours=$((total_seconds / 3600))
    local minutes=$(((total_seconds % 3600) / 60))
    local seconds=$((total_seconds % 60))
    printf "%02d:%02d:%02d\n" $hours $minutes $seconds
}

if [[ -z "$SNAPSHOTDIR" ]]; then
  echo "Snapshot dir not set, please run the script from within the Makefile, or check your .env" && exit 1
fi

start_time=$(date +%s)

echo -e "$BLUE$(date "$TIMESTAMP_FORMAT") SNAPSHOTDIR=$SNAPSHOTDIR$NC"
echo -e "\n"

if [ -z "$(ls -A ${SNAPSHOTDIR%/*})" ]; then
    echo -e "$RED$(date "$TIMESTAMP_FORMAT") No snapshot files found in ${SNAPSHOTDIR%/*}$NC"
    exit 1
fi

# Iterate through the files in SNAPSHOTDIR and process each one
for f in $SNAPSHOTDIR; do
    if [ -f "$f" ]; then
        echo -e "$YELLOW$(date "$TIMESTAMP_FORMAT") Processing $f...$NC"
        # note: accounts selector config is copied directly in the Dockerfile.
        # due to that we can hardocde the path here.
        CMD="./solana-snapshot-etl \"$f\" --accounts-selector-config=accounts-selector-config.json"
        echo -e "$BLUE$(date "$TIMESTAMP_FORMAT") Running command: $CMD$NC"

        eval $CMD

        if [ $? -ne 0 ]; then
            echo -e "$RED$(date "$TIMESTAMP_FORMAT") Error processing $f$NC"
            exit 1
        fi

        echo -e "$GREEN$(date "$TIMESTAMP_FORMAT") Completed processing $f$NC"
    else
        echo -e "$RED$(date "$TIMESTAMP_FORMAT") No valid files found in $SNAPSHOTDIR$NC"
        exit 1
    fi
done

end_time=$(date +%s)
execution_time=$((end_time - start_time))
formatted_time=$(convert_time $execution_time)

echo -e "$GREEN$(date "$TIMESTAMP_FORMAT") Total execution time: $formatted_time$NC"
echo -e "$GREEN$(date "$TIMESTAMP_FORMAT") All snapshot files processed successfully.$NC"
