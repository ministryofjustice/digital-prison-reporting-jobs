#!/bin/bash

# Script to restore a missing record
# script.
#
# Usage:
#
#   scripts/start-glue-streaming-job.sh <glue-job-name>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
STREAMING_JOB_NAME=$1

echo "Starting Glue streaming job ${STREAMING_JOB_NAME}"
aws glue start-job-run --job-name "$STREAMING_JOB_NAME" > /dev/null
