#!/bin/bash

# Script to resume a DMS task
# script.
#
# Usage:
#
#   scripts/resume-dms-task.sh <dms-task-name>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
DMS_TASK_NAME=$1

# Get DMS task ARN
DMS_TASK_ARN="$(aws dms describe-replication-tasks --filters Name=replication-task-id,Values="$DMS_TASK_NAME" | jq '.ReplicationTasks[].ReplicationTaskArn' | tr -d '"')"

echo "Resuming DMS task"
aws dms start-replication-task --replication-task-arn "$DMS_TASK_ARN" --start-replication-task-type resume-processing
