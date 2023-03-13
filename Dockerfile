FROM python:3.10-slim

WORKDIR /app
#RUN apt-get update && apt-get install -y cargo
RUN pip3 install poetry
COPY pyproject.toml pyproject.toml
RUN poetry install
COPY cherrydb_meta cherrydb_meta

CMD ["poetry", "run", "gunicorn", "-w", "4", "--access-logfile", "-", "-k", "uvicorn.workers.UvicornWorker", "cherrydb_meta.main:app"]
