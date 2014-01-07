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

        EXECUTE format('CREATE TEMP TABLE tmp (LIKE wh_nagios.%I)', v_partname);

        EXECUTE format('WITH list AS (SELECT date_records, count(*) AS num
                FROM wh_nagios.%I
                GROUP BY date_records
            ),
            del AS (DELETE FROM wh_nagios.%I c
                USING list l WHERE c.date_records = l.date_records AND l.num > 1
                RETURNING c.*
            ),
            rec AS (SELECT date_records, (unnest(records)).*
                FROM del
            )
            INSERT INTO tmp
            SELECT date_records, array_agg(row(timet,value)::wh_nagios.counters_detail)
            FROM rec
            GROUP BY date_records
            UNION ALL
            SELECT cd.* FROM wh_nagios.%I cd JOIN list l USING (date_records)
            WHERE num = 1;
        ', v_partname, v_partname, v_partname, v_partname);
        EXECUTE format('TRUNCATE wh_nagios.%I', v_partname) ;
        EXECUTE format('INSERT INTO wh_nagios.%I SELECT * FROM tmp', v_partname) ;
        DROP TABLE tmp;
    END LOOP ;
    UPDATE wh_nagios.services SET last_cleanup = now() WHERE id = p_serviceid ;

    RETURN true ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;
