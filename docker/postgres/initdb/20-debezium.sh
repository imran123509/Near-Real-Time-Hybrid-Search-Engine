#!/usr/bin/env bash
# Prepares the database for Debezium with the least privilege it needs:
#
#   - a login role with the REPLICATION attribute, which lets it open a logical
#     replication slot, and SELECT on the captured table for the snapshot;
#   - the publication pgoutput streams from, created here because the
#     connector runs with publication.autocreate.mode=disabled and so needs
#     no ownership of the table.
#
# The publication name must match "publication.name" in
# docker/debezium/connector.json.

: "${DEBEZIUM_DB_USER:?DEBEZIUM_DB_USER must be set}"
: "${DEBEZIUM_DB_PASSWORD:?DEBEZIUM_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v dbz_user="$DEBEZIUM_DB_USER" \
    -v dbz_password="$DEBEZIUM_DB_PASSWORD" \
    -v db_name="$POSTGRES_DB" <<'SQL'
CREATE ROLE :"dbz_user" WITH LOGIN REPLICATION PASSWORD :'dbz_password';
GRANT CONNECT ON DATABASE :"db_name" TO :"dbz_user";
GRANT USAGE ON SCHEMA public TO :"dbz_user";
GRANT SELECT ON public.documents TO :"dbz_user";

CREATE PUBLICATION search_documents FOR TABLE public.documents;
SQL
