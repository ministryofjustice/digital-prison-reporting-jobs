#!/bin/bash

# Script to push a record to kinesis
# script.
#
# Usage:
#
#   scripts/push-record-to-kinesis.sh <kinesis-stream-name> <record-data-json-file>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
KINESIS_STREAM_NAME=$1
RECORD_DATA_JSON_FILE=$2

aws kinesis put-record --stream-name "$KINESIS_STREAM_NAME" --partition-key 1 --data "file://$RECORD_DATA_JSON_FILE"
