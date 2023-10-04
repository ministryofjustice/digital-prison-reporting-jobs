#!/bin/bash

# Script to stop a DMS task
# script.
#
# Usage:
#
#   scripts/stop-dms-task.sh <dms-task-name>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
DMS_TASK_NAME=$1

# Get DMS task ARN
DMS_TASK_ARN="$(aws dms describe-replication-tasks --filters Name=replication-task-id,Values="$DMS_TASK_NAME" | jq '.ReplicationTasks[].ReplicationTaskArn' | tr -d '"')"

echo "Stopping DMS task ${DMS_TASK_NAME}"
aws dms stop-replication-task --replication-task-arn "$DMS_TASK_ARN" >/dev/null
