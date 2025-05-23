#!/bin/bash

PERIOD="15m"
YEAR="2025"
SYMBOLS=("SUSHIUSDT" "1INCHUSDT" "COMPUSDT" "XLMUSDT" "XMRUSDT")
#SYMBOLS=("UNIUSDT" "LTCUSDT" "LINKUSDT")

#https://data.binance.vision/data/futures/um/monthly/klines/BTCUSDT/15m/BTCUSDT-15m-2025-02.zip

for SYMBOL in "${SYMBOLS[@]}"; do
  BASE_URL="https://data.binance.vision/data/futures/um/monthly/klines/${SYMBOL}/${PERIOD}"
  TARGET_DIR="./${SYMBOL}/${PERIOD}"
  mkdir -p "$TARGET_DIR"

  for MONTH in {01..04}; do
      FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
      URL="${BASE_URL}/${FILE_NAME}"
      echo "Downloading ${URL}..."
      curl -o "$TARGET_DIR/$FILE_NAME" "$URL"

      FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
      unzip -o "$TARGET_DIR/$FILE_NAME" -d $TARGET_DIR

      FILE_NAME="${SYMBOL}-${PERIOD}-${YEAR}-${MONTH}.zip"
      rm "$TARGET_DIR/$FILE_NAME"
  done

  file_count=$(find "$SYMBOL" -type f | wc -l)
  echo "Total files in '$SYMBOL': $file_count"
done
