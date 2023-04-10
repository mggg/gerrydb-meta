FROM python:3.10-slim

# See Google Cloud Run + FUSE configuration template:
# https://cloud.google.com/run/docs/tutorials/network-filesystems-fuse
RUN set -e; \
    apt-get update -y && apt-get install -y \
    tini \
    lsb-release; \
    gcsFuseRepo=gcsfuse-`lsb_release -c -s`; \
    echo "deb http://packages.cloud.google.com/apt $gcsFuseRepo main" | \
    tee /etc/apt/sources.list.d/gcsfuse.list; \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
    apt-key add -; \
    apt-get update; \
    apt-get install -y gcsfuse \
    && apt-get clean
ENV MNT_DIR /mnt/gcs

WORKDIR /app
RUN pip3 install poetry
COPY pyproject.toml pyproject.toml
# https://stackoverflow.com/a/54763270
RUN POETRY_VIRTUALENVS_CREATE=false poetry install --no-interaction --no-ansi
COPY cherrydb_meta cherrydb_meta

ENTRYPOINT ["/usr/bin/tini", "--"] 
CMD ["/app/serve.sh"]
