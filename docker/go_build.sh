#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
OUTPUT_BINARY="${OPERATOR_BIN}" \
MAIN_SRC_FILE="${CUR_DIR}/../cmd/manager/main.go"

if CGO_ENABLED=0 GO111MODULE=on GOOS=linux GOARCH=amd64 go build \
    -o "${OUTPUT_BINARY}" \
    "${MAIN_SRC_FILE}"
then
    echo "Build OK"
else
    echo "WARNING! BUILD FAILED!"
    exit 1
fi