#!/usr/bin/env bash
set -eo pipefail

# See template: https://cloud.google.com/run/docs/tutorials/network-filesystems-fuse
# Create mount directory for service
mkdir -p $MNT_DIR

echo "Mounting GCS Fuse."
gcsfuse --debug_gcs --debug_fuse $BUCKET $MNT_DIR 
echo "Mounting completed."

exec gunicorn -w 1 --access-logfile - -k uvicorn.workers.UvicornWorker cherrydb_meta.main:app --timeout 300
