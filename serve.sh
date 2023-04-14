#!/usr/bin/env bash
set -eo pipefail

# ogr2ogr configuration for Cloud Run: write PostgreSQL credentials
# from environment variables to a connection service file so that
# they're less likely to be leaked in application logs.
echo "[gerrydb]
host=${INSTANCE_CONNECTION_NAME}
user=${DB_USER}
dbname=${DB_NAME}
password=${DB_PASS}
" >> $HOME/.pg_service.conf

gunicorn -w 4 \
    --access-logfile - \
    -k uvicorn.workers.UvicornWorker \
    gerrydb_meta.main:app \
    --timeout 300
