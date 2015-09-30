-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION  wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group


CREATE OR REPLACE
FUNCTION wh_nagios.merge_service( p_service_src bigint, p_service_dst bigint, drop_old boolean DEFAULT FALSE)
RETURNS boolean
LANGUAGE plpgsql
AS $$
DECLARE
    r_ok record ;
    r_service record ;
    r_metric_src record ;
    r_metric_dst record ;
    v_old_id_metric bigint ;
    v_new_id_metric bigint ;
BEGIN
    -- Does the two services exists, and are they from the same server ?
    SELECT COUNT(*) = 2 AS num_services,
        COUNT(DISTINCT id_server) = 1 AS num_distinct_servers
        INTO r_ok
    FROM wh_nagios.services
    WHERE id IN ( p_service_src, p_service_dst );

    IF ( NOT r_ok.num_services
        OR NOT r_ok.num_distinct_servers
    ) THEN
        RETURN false ;
    END IF ;

    SELECT * INTO r_service FROM wh_nagios.services WHERE id = p_service_src ;

    FOR r_metric_src IN SELECT * FROM wh_nagios.metrics WHERE id_service = p_service_src LOOP
        v_old_id_metric = r_metric_src.id ;
        SELECT * INTO r_metric_dst FROM wh_nagios.metrics
            WHERE id_service = p_service_dst
            AND label = r_metric_src.label
            AND unit = r_metric_src.unit ;
        IF r_metric_dst IS NULL THEN
            -- Create a new metric
            SELECT nextval('public.metrics_id_seq'::regclass) INTO v_new_id_metric ;
            r_metric_src.id = v_new_id_metric ;
            INSERT INTO wh_nagios.metrics (id, id_service, label, unit, tags, min, max, critical, warning)
            VALUES (v_new_id_metric, p_service_dst, r_metric_src.label, r_metric_src.unit, r_metric_src.tags, r_metric_src.min, r_metric_src.max, r_metric_src.critical, r_metric_src.warning);
        ELSE
            v_new_id_metric = r_metric_dst.id ;
        END IF ;
        -- merge data from the two metrics
        EXECUTE format('
            INSERT INTO wh_nagios.counters_detail_%s
                SELECT * FROM wh_nagios.counters_detail_%s',
        v_new_id_metric, v_old_id_metric) ;
    END LOOP ;
    -- update metadata
    WITH meta AS (
        SELECT min(oldest_record) AS oldest,
            max(newest_record) AS newest
        FROM wh_nagios.services
        WHERE id IN ( p_service_src, p_service_dst )
    )
    UPDATE wh_nagios.services s
    SET oldest_record = meta.oldest, newest_record = meta.newest
    FROM meta
    WHERE s.id = p_service_dst ;

    IF drop_old THEN
        DELETE FROM wh_nagios.services WHERE id= p_service_src ;
    END IF;
    PERFORM public.create_graph_for_new_metric( s.id_server )
        FROM (
            SELECT id_server FROM wh_nagios.services
            WHERE id = p_service_src
        ) s;
    RETURN true ;

END ;
$$ ;

REVOKE ALL ON FUNCTION wh_nagios.merge_service( bigint, bigint, boolean ) FROM public ;
COMMENT ON FUNCTION wh_nagios.merge_service( bigint, bigint, boolean ) IS
'Merge data from a wh_nagios service into another.' ;

/* fix ACL on wh_nagios.hub_reject, see issue #26 */
CREATE OR REPLACE
FUNCTION wh_nagios.grant_dispatcher(IN p_rolname name)
RETURNS TABLE (operat text, approle name, appright text, objtype text, objname text)
LANGUAGE plpgsql STRICT VOLATILE LEAKPROOF SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
    v_dbname name := pg_catalog.current_database();
BEGIN
    operat   := 'GRANT';
    approle  := p_rolname;

    appright := 'CONNECT';
    objtype  := 'DATABASE';
    objname  := v_dbname;
    EXECUTE pg_catalog.format('GRANT %s ON %s %I TO %I', appright, objtype, objname, approle);
    RETURN NEXT;

    appright := 'USAGE';
    objtype  := 'SCHEMA';
    objname  := 'wh_nagios';
    EXECUTE pg_catalog.format('GRANT %s ON %s %I TO %I', appright, objtype, objname, approle);
    RETURN NEXT;

    appright := 'USAGE';
    objtype  := 'SEQUENCE';
    objname  := 'wh_nagios.hub_id_seq';
    EXECUTE pg_catalog.format('GRANT %s ON %s %s TO %I', appright, objtype, objname, approle);
    RETURN NEXT;

    appright := 'INSERT';
    objtype  := 'TABLE';
    objname  := 'wh_nagios.hub';
    EXECUTE pg_catalog.format('GRANT %s ON %s %s TO %I', appright, objtype, objname, approle);
    RETURN NEXT;

    appright := 'INSERT';
    objtype  := 'TABLE';
    objname  := 'wh_nagios.hub_reject';
    EXECUTE pg_catalog.format('GRANT %s ON %s %s TO %I', appright, objtype, objname, approle);
    RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name) FROM public;

COMMENT ON FUNCTION wh_nagios.grant_dispatcher(IN name)
    IS 'Grant a role to dispatch performance data in warehouse wh_nagios.';


-- This line must be the last one, so that every functions are owned
-- by the database owner
SELECT * FROM public.set_extension_owner('wh_nagios');
