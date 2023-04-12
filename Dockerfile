FROM python:3.10-slim

WORKDIR /app
RUN pip3 install poetry
COPY pyproject.toml pyproject.toml
# https://stackoverflow.com/a/54763270
RUN POETRY_VIRTUALENVS_CREATE=false poetry install --no-interaction --no-ansi
COPY gerrydb_meta gerrydb_meta

CMD ["gunicorn", "-w", "4", "--access-logfile", "-", "-k", "uvicorn.workers.UvicornWorker", "gerrydb_meta.main:app", "--timeout", "300"]
