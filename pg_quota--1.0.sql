/* Installation script for pg_quota extension */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_quota" to load this file. \quit

CREATE SCHEMA quota;

set search_path='quota';

CREATE FUNCTION get_quota_status(relid OUT oid, space_used OUT int8, quota OUT int8)
RETURNS SETOF record STRICT
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE VIEW quota.status AS
SELECT relid::regclass AS tablename, space_used, quota
FROM get_quota_status();

-- Configuration table
create table quota.config (relationid oid PRIMARY key, quota int8);

SELECT pg_catalog.pg_extension_config_dump('quota.config', '');

reset search_path;
