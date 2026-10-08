#!/bin/bash
# Runs once, on the first start of the postgres container (empty volume).
# Creates the three databases and their roles, then applies each schema.
set -euo pipefail
psql=(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER")

"${psql[@]}" --dbname postgres <<-SQL
    CREATE ROLE shop       LOGIN PASSWORD 'shop' REPLICATION;   -- Debezium reads the WAL as this role
    CREATE ROLE loader     LOGIN PASSWORD 'loader';
    CREATE ROLE api_reader LOGIN PASSWORD 'api_reader';
    CREATE ROLE airflow    LOGIN PASSWORD 'airflow';
    CREATE DATABASE shop      OWNER shop;
    CREATE DATABASE warehouse OWNER loader;
    CREATE DATABASE airflow   OWNER airflow;
SQL

PGPASSWORD=shop   psql -v ON_ERROR_STOP=1 -U shop   -d shop      -f /docker-entrypoint-initdb.d/schema/shop.sql
PGPASSWORD=loader psql -v ON_ERROR_STOP=1 -U loader -d warehouse -f /docker-entrypoint-initdb.d/schema/warehouse.sql
