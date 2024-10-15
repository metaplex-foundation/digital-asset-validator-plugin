#!/bin/bash

TIMESTAMP_FORMAT='+%Y-%m-%d %H:%M:%S'

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

start_time=$(date +%s)

# Update the geyser-conf.json dynamically using environment variables
echo -e "${BLUE}$(date "$TIMESTAMP_FORMAT") BASEDIR=${BASEDIR}, GEYSER_CONFIG=${GEYSER_CONFIG}, TCP_PORT=${TCP_PORT}, TCP_MIN_SUBSCRIBERS=${TCP_MIN_SUBSCRIBERS}${NC}"
echo -e "${GREEN}$(date "$TIMESTAMP_FORMAT") Updated geyser-conf.json:${NC}"
sed -i "s/\"tcp_port\": [0-9]*/\"tcp_port\": ${TCP_PORT}/" ${GEYSER_CONFIG}
sed -i "s/\"tcp_min_subscribers\": [0-9]*/\"tcp_min_subscribers\": ${TCP_MIN_SUBSCRIBERS}/" ${GEYSER_CONFIG}
cat ${GEYSER_CONFIG}

# Check if BASEDIR is empty
if [ -z "$(ls -A ${BASEDIR} 2>/dev/null)" ]; then
    echo -e "${RED}$(date "$TIMESTAMP_FORMAT") No snapshot files found in ${BASEDIR}${NC}"
    exit 1
fi

if [ -z "$(ls -A ${BASEDIR%/*})" ]; then
    echo -e "${RED}$(date "$TIMESTAMP_FORMAT") No snapshot files found in ${BASEDIR%/*}${NC}"
    exit 1
fi

# Iterate through the files in BASEDIR and process each one
for f in ${BASEDIR}; do
    if [ -f "$f" ]; then
        echo -e "${YELLOW}$(date "$TIMESTAMP_FORMAT") Processing $f...${NC}"
        CMD="solana-snapshot-etl \"$f\" --geyser=${GEYSER_CONFIG}"
        echo -e "${BLUE}$(date "$TIMESTAMP_FORMAT") Running command: $CMD${NC}"

        eval $CMD

        if [ $? -ne 0 ]; then
            echo -e "${RED}$(date "$TIMESTAMP_FORMAT") Error processing $f${NC}"
            exit 1
        fi

        echo -e "${GREEN}$(date "$TIMESTAMP_FORMAT") Completed processing $f${NC}"
    else
        echo -e "${RED}$(date "$TIMESTAMP_FORMAT") No valid files found in ${BASEDIR}${NC}"
        exit 1
    fi
done

end_time=$(date +%s)
execution_time=$((end_time - start_time))
formatted_time=$(convert_time $execution_time)

echo -e "${GREEN}$(date "$TIMESTAMP_FORMAT") Total execution time: ${formatted_time}${NC}"
echo -e "${GREEN}$(date "$TIMESTAMP_FORMAT") All snapshot files processed successfully.${NC}"