#!/bin/bash

# Script to refresh a domain
# script.
#
# Usage:
#
#   scripts/refresh-domain.sh <domain-refresh-job-name> <domain-name> <domain-table-name> <domain-operation>
#
# Requires the aws cli to be installed and configured.

set -e

# Args
DOMAIN_REFRESH_JOB_NAME=$1
DOMAIN_NAME=$2
DOMAIN_TABLE_NAME=$3
DOMAIN_OPERATION=$4


echo "Starting refresh job for domain table"
aws glue start-job-run --job-name "$DOMAIN_REFRESH_JOB_NAME" --arguments="--dpr.domain.name=${DOMAIN_NAME}","--dpr.domain.table.name=${DOMAIN_TABLE_NAME}","--dpr.domain.operation=${DOMAIN_OPERATION}" > /dev/null
