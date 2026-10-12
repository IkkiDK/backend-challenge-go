#!/bin/sh
# Creates the restricted login role the service connects with. Migrations run as
# the database owner and grant this role only the privileges it needs, so the
# application can never alter the schema, disable triggers or rewrite the ledger.
#
# The postgres entrypoint may source this file instead of executing it, so it
# avoids changing shell options and relies on psql's ON_ERROR_STOP for failures.

: "${WALLET_APP_DB_PASSWORD:?WALLET_APP_DB_PASSWORD must be set}"

psql -v ON_ERROR_STOP=1 \
	--username "$POSTGRES_USER" \
	--dbname "$POSTGRES_DB" \
	--set app_password="$WALLET_APP_DB_PASSWORD" \
	--set db_name="$POSTGRES_DB" <<'EOSQL'
CREATE ROLE wallet_app LOGIN PASSWORD :'app_password';
GRANT CONNECT ON DATABASE :"db_name" TO wallet_app;
EOSQL
