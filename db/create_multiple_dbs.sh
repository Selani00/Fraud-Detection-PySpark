#!/bin/bash
set -e

echo "Creating databases and users..."

# Create frauddb database and appuser
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" << EOSQL
CREATE DATABASE frauddb;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" << EOSQL
CREATE USER appuser WITH PASSWORD 'apppassword';
GRANT ALL PRIVILEGES ON DATABASE frauddb TO appuser;
ALTER DATABASE frauddb OWNER TO appuser;
EOSQL

# Create airflow database and airflow user
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" << EOSQL
CREATE DATABASE airflow;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" << EOSQL
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;
EOSQL

# Grant schema permissions
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "frauddb" << EOSQL
GRANT ALL ON SCHEMA public TO appuser;
EOSQL

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" << EOSQL
GRANT ALL ON SCHEMA public TO airflow;
EOSQL

echo "Databases and users created successfully"
