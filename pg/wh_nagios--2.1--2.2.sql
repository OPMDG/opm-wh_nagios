-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_id_seq', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_reject', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_reject_id_seq', '') ;

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group

CREATE OR REPLACE
FUNCTION wh_nagios.get_metric_data(id_metric bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone)
RETURNS TABLE(timet timestamp with time zone, value numeric)
LANGUAGE plpgsql STABLE STRICT SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    -- FIXME check user rights to access these data ?
    RETURN QUERY EXECUTE format('
        SELECT * FROM (
            SELECT (pg_catalog.unnest(records)).*
            FROM wh_nagios.counters_detail_%s
            WHERE date_records >= date_trunc(''day'',$1)
                AND date_records <= date_trunc(''day'',$2)
        ) sql
        WHERE timet >= $1 AND timet <= $2', id_metric
    ) USING timet_begin,timet_end;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone)
    FROM public;

COMMENT ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone) IS
'Return metric data for the specified metric unique identifier within the specified interval.';

