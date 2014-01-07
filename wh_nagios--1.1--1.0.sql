-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2013: Open PostgreSQL Monitoring Development Group

SET statement_timeout TO 0 ;

CREATE OR REPLACE FUNCTION cleanup_service(p_serviceid bigint)
    RETURNS boolean
    AS $$
DECLARE
  v_servicefound boolean ;
  v_partid bigint ;
  v_partname text ;
BEGIN
    SELECT ( COUNT(*) = 1 ) INTO v_servicefound FROM wh_nagios.services WHERE id = p_serviceid;
    IF NOT v_servicefound THEN
        RETURN false ;
    END IF ;
    FOR v_partid IN SELECT id FROM wh_nagios.labels WHERE id_service = p_serviceid LOOP
        v_partname := format('counters_detail_%s', v_partid) ;

        EXECUTE format('WITH list AS (SELECT date_records
                FROM wh_nagios.%I
                GROUP BY date_records
                HAVING COUNT(*) > 1
            ),
            del AS (DELETE FROM wh_nagios.%I c
                USING list l WHERE c.date_records = l.date_records
                RETURNING *
            ),
            rec AS (SELECT date_records, (unnest(cd.records)).*
                FROM list l
                JOIN wh_nagios.%I cd USING (date_records)
            )
            INSERT INTO wh_nagios.%I
            SELECT date_records, array_agg(row(timet,value)::wh_nagios.counters_detail)
            FROM rec
            GROUP BY date_records;
        ', v_partname, v_partname, v_partname, v_partname);
    END LOOP ;
    UPDATE wh_nagios.services SET last_cleanup = now() WHERE id = p_serviceid ;

    RETURN true ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;
