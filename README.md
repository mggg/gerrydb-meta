# GerryDB metadata server

This repository contains the code and deployment configuration for GerryDB's metadata server, which is a simple FastAPI application in front of a PostGIS database. PostGIS is the server's sole external dependency.

## Running the server
1. In the root of this repository, install dependencies with `poetry install`.
2. In the root of this repository, launch a PostGIS server with `docker-compose up -d`.
3. Connect to the PostGIS server with a Postgres client (port `54320`, username `postgres`, password `dev`).
4. Initialize a PostGIS database by executing the SQL statement `CREATE DATABASE gerrydb`. Close the connection, make `gerrydb` the defualt database. Reopen the connection, and add geography database objects via `CREATE EXTENSION postgis`.
5. Initialize the application schema by running `GERRYDB_DATABASE_URI=postgresql://postgres:dev@localhost:54320/gerrydb python init.py --reset --name <NAME> --email <email>`. Save the generated API key to your bash profile (for some Mac users, this might be acheived by `cat .gerryrc >> ~/.zprofile`). Also store the following code in the profile: 
```
export GERRYDB_DATABASE_URI="postgresql://postgres:dev@localhost:54320/gerrydb"
export GERRYDB_TEST_DATABASE_URI="postgresql://postgres:test@localhost:54321"
source <wherever you keep your code>/gerrydb-meta/.gerryrc
```
Be sure to source the profile again.

6. You need a `$HOME/.gerrydb/config` file that reads something like this (if you want to connect locally
and not to production). 
```
[default]
host = "localhost:8000"
key = <API_key>
```
If this file does not exist yet, initializing the database should have created it in your home directory. 
If it already exists, you will be given the option to overwrite it, or leave it.
If you leave, you may need to manually edit it.

7. Run the application server with `uvicorn gerrydb_meta.main:app --reload`.

## Deleting the database
If you want to clear your database and start over, delete the relevant docker containers and volumes, then follow the steps above.
Note that your profile will still have your old API keys stored in it, but your most recent one
will be appended to the end.

## Running tests
The Docker Compose manifest for this project spins up two PostGIS instances: one for persisting data for long-term local development, and one for ephemeral use by unit tests. The test server is exposed on port 54321; the username is `postgres`, and the password is `test`. To run the test suite, set up the app server as described above, initialize the test database by repeating step 4 (`CREATE DATABASE gerrydb; CREATE EXTENSION postgis;`), and execute the test suite with
```sh
GERRYDB_DATABASE_URI=postgresql://postgres:test@localhost:54321/gerrydb python -m pytest
```

It is not necessary to initialize the application schema within this test database—all tables are dropped and recreated between test suite runs.
