#!/usr/bin/env bash

set -eux

environment="test"
BASE_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]:-$0}"; )" &> /dev/null && pwd 2> /dev/null; )";
raw_load_dir="${BASE_DIR}/raw-load-only"

tables=("agency_internal_locations" "agency_locations" "movement_reasons" "offender_bookings" "offender_external_movements" "offenders")

for table_name in "${tables[@]}"
do
  symlink_file="${raw_load_dir}/${table_name}_load_only_symlinks.txt"
  s3_destination="s3://dpr-raw-zone-${environment}/nomis/${table_name}/_symlink_format_load_only_manifest/${table_name}_load_only_symlinks.txt"
  aws s3 cp "${symlink_file}" "$s3_destination}"
done