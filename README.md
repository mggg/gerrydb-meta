# GerryDB metadata server

This repository contains the code and deployment configuration for GerryDB's metadata server, which is a simple FastAPI application in front of a PostGIS database. PostGIS is the server's sole external dependency.

## Running the server
1. Install dependencies with `poetry install`.
2. In the root of this repository, launch a PostGIS server with `docker-compose up -d`.
3. Connect to the PostGIS server with a Postgres client (port `54320`, username `postgres`, password `dev`).
4. Initialize a PostGIS database by executing the SQL statement `CREATE DATABASE gerrydb; CREATE EXTENSION postgis;`
5. Initialize the application schema by running `GERRYDB_DATABASE_URI=postgresql://postgres:dev@localhost:54320/gerrydb python init.py`. Save the generated API key.
6. Run the application server with `uvicorn gerrydb_meta.main:app --reload`.

## Running tests
The Docker Compose manifest for this project spins up two PostGIS instances: one for persisting data for long-term local development, and one for ephemeral use by unit tests. The test server is exposed on port 54321; the username is `postgres`, and the password is `test`. To run the test suite, set up the app server as described above, initialize the test database by repeating step 4 (`CREATE DATABASE gerrydb; CREATE EXTENSION postgis;`), and execute the test suite with `GERRYDB_DATABASE_URI=postgresql://postgres:test@localhost:54321/gerrydb python -m pytest`. It is not necessary to initialize the application schema within this test database—all tables are dropped and recreated between test suite runs.
