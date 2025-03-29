#!/bin/bash

PERIOD="15m"
YEAR="2025"
SYMBOL="AAVEUSDT"
BASE_URL="https://data.binance.vision/data/futures/um/monthly/klines/${SYMBOL}/${PERIOD}"

TARGET_DIR="./${SYMBOL}/${PERIOD}"
mkdir -p "$TARGET_DIR"

# download
for MONTH in {01..12}; do
    FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
    URL="${BASE_URL}/${FILE_NAME}"
    echo "Downloading ${URL}..."
    curl -o "$TARGET_DIR/$FILE_NAME" "$URL"
done

echo "Download complete."

# unzip
for MONTH in {01..12}; do
    FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
    unzip -o "$TARGET_DIR/$FILE_NAME" -d $TARGET_DIR
done

# rm zip
for MONTH in {01..12}; do
    FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
    rm "$TARGET_DIR/$FILE_NAME"
done
