-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION  wh_nagios UPDATE" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2022: Open PostgreSQL Monitoring Development Group

CREATE OR REPLACE
FUNCTION wh_nagios.drop_partition_on_delete_service()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DROP TABLE wh_nagios.service_counters_%s', OLD.id) ;
    RETURN OLD;
EXCEPTION
    WHEN undefined_table THEN
        RETURN NULL;
END
$$;

/* wh_nagios.delete_metrics(VARIADIC bigint[])
Delete specific metrics.

@p_metricsid: Unique identifiers of the metrics to delete.
@return : the metrics id deleted
*/
CREATE OR REPLACE
FUNCTION wh_nagios.delete_metrics(VARIADIC p_metricsid bigint[])
RETURNS TABLE (id_metric bigint)
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
    v_id_metric bigint;
    v_id_service bigint;
BEGIN
    IF NOT public.is_admin() THEN
        RAISE EXCEPTION 'You must be an admin!';
    END IF;

    -- First, delete all records for the given metrics in the underlying
    -- partition table.
    FOREACH v_id_metric IN ARRAY p_metricsid LOOP
        SELECT id_service INTO v_id_service
          FROM wh_nagios.metrics
          WHERE id = v_id_metric;

        -- Ignore this metric if it doesn't exist.
        CONTINUE WHEN NOT FOUND;

        -- Delete all rows for this metric.
        EXECUTE format('DELETE FROM wh_nagios.service_counters_%s'
            || ' WHERE id_metric = %s', v_id_service, v_id_metric);
    END LOOP;

    -- Finally, remove the row from the metrics table.
    RETURN QUERY DELETE FROM wh_nagios.metrics
        WHERE id = ANY ( p_metricsid )
        RETURNING id AS id_metric;
END
$$;
