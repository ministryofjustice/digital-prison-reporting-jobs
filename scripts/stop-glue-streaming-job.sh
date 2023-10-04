#!/bin/bash

# Script to restore a missing record
# script.
#
# Usage:
#
#   scripts/stop-glue-streaming-job.sh <glue-job-name>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
STREAMING_JOB_NAME=$1

# Get Glue streaming job run id
STREAMING_JOB_RUN_ID="$(aws glue get-job-runs --job-name "$STREAMING_JOB_NAME" | jq '.JobRuns[0].Id' | tr -d '"')"

echo "Stopping Glue streaming job ${STREAMING_JOB_NAME}"
aws glue batch-stop-job-run --job-name "$STREAMING_JOB_NAME" --job-run-ids "${STREAMING_JOB_RUN_ID}" > /dev/null
