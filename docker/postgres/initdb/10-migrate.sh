#!/usr/bin/env bash
# Applies the project's migrations in order, so the local database uses exactly
# the schema in migrations/ rather than a copy of it.
#
# Like every script here, it runs once, when the data volume is first
# initialised. To run it again: docker compose down -v
#
# The postgres image may source this file rather than execute it, so it sets no
# shell options of its own; the image's entrypoint already stops on errors.

for migration in /migrations/*.up.sql; do
    echo "applying ${migration##*/}"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --file "$migration"
done
