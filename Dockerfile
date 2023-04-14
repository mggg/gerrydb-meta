FROM python:3.10-slim

WORKDIR /app
RUN apt-get update && apt-get -y install gdal-bin
RUN pip3 install poetry
COPY pyproject.toml pyproject.toml
# https://stackoverflow.com/a/54763270
RUN POETRY_VIRTUALENVS_CREATE=false poetry install --no-interaction --no-ansi
COPY gerrydb_meta gerrydb_meta
COPY serve.sh serve.sh
RUN chmod +x serve.sh

CMD ["/app/serve.sh"]

