-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "ALTER EXTENSION  wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2018: Open PostgreSQL Monitoring Development Group


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

    appright := 'EXECUTE';
    objtype  := 'FUNCTION';
    objname  := 'wh_nagios.dispatch_record(integer, bool)';
    EXECUTE pg_catalog.format('GRANT %s ON %s %s TO %I', appright, objtype, objname, approle);
    RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name) FROM public;

COMMENT ON FUNCTION wh_nagios.grant_dispatcher(IN name)
    IS 'Grant a role to dispatch performance data in warehouse wh_nagios.';


/* wh_nagios.dispatch_record(boolean, integer)
Dispatch records from wh_nagios.hub into counters_detail_$ID

$ID is found in wh_nagios.services_metric and wh_nagios.services, with correct hostname,servicedesc and label

@log_error: If true, will report errors and details in wh_nagios.hub_reject
@return : true if everything went well.
*/
CREATE OR REPLACE
FUNCTION wh_nagios.dispatch_record(num_lines integer DEFAULT 5000, log_error boolean DEFAULT false,
    OUT processed bigint, OUT failed bigint)
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
    --Select current lines and lock them so then can be deleted
    --Use NOWAIT so there can't be two concurrent processes
    c_hub       CURSOR FOR SELECT * FROM wh_nagios.hub LIMIT num_lines FOR UPDATE NOWAIT;
    r_hub       record;
    i           integer;
    cur         hstore;
    msg_err     text;
    servicesrow wh_nagios.services%ROWTYPE;
    metricsrow  wh_nagios.metrics%ROWTYPE;
    serversrow  public.servers%ROWTYPE;
    updates     hstore[2];
BEGIN
/*
TODO: Handle seracl
*/
    processed := 0;
    failed    := 0;

    BEGIN
        FOR r_hub IN c_hub LOOP
            msg_err := NULL;

            --Check 1 dimension,at least 10 vals and even number of data
            IF ( (pg_catalog.array_upper(r_hub.data, 2) IS NOT NULL)
                OR (pg_catalog.array_upper(r_hub.data, 1) < 10)
                OR ((pg_catalog.array_upper(r_hub.data, 1) % 2) <> 0)
            ) THEN
                IF log_error THEN
                    msg_err := NULL;
                    IF (pg_catalog.array_upper(r_hub.data, 2) IS NOT NULL) THEN
                        msg_err := COALESCE(msg_err,'') || 'given array has more than 1 dimension';
                    END IF;
                    IF (pg_catalog.array_upper(r_hub.data, 1) <= 9) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'less than 10 values';
                    END IF;
                    IF ((pg_catalog.array_upper(r_hub.data, 1) % 2) != 0) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'number of parameter not even';
                    END IF;

                    INSERT INTO wh_nagios.hub_reject (id, rolname, data,msg) VALUES (r_hub.id, r_hub.rolname, r_hub.data, msg_err);
                END IF;

                failed := failed + 1;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;

                CONTINUE;
            END IF;

            cur := NULL;
            --Get all data as hstore,lowercase
            FOR i IN 1..pg_catalog.array_upper(r_hub.data, 1) BY 2 LOOP
                IF (cur IS NULL) THEN
                    cur := hstore(pg_catalog.lower(r_hub.data[i]), r_hub.data[i+1]);
                ELSE
                    cur := cur || hstore(pg_catalog.lower(r_hub.data[i]), r_hub.data[i+1]);
                END IF;
            END LOOP;

            serversrow  := NULL;
            servicesrow := NULL;
            metricsrow  := NULL;

            --Do we have all informations needed ?
            IF ( ((cur->'hostname') IS NULL)
                OR ((cur->'servicedesc') IS NULL)
                OR ((cur->'label') IS NULL)
                OR ((cur->'timet') IS NULL)
                OR ((cur->'value') IS NULL)
            ) THEN
                IF log_error THEN
                    msg_err := NULL;
                    IF ((cur->'hostname') IS NULL) THEN
                        msg_err := COALESCE(msg_err,'') || 'hostname required';
                    END IF;
                    IF ((cur->'servicedesc') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'servicedesc required';
                    END IF;
                    IF ((cur->'label') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'label required';
                    END IF;
                    IF ((cur->'timet') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'timet required';
                    END IF;
                    IF ((cur->'value') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'value required';
                    END IF;

                    INSERT INTO wh_nagios.hub_reject (id, rolname, data, msg)
                    VALUES (r_hub.id, r_hub.rolname, r_hub.data, msg_err);
                END IF;

                failed := failed + 1;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;

                CONTINUE;
            END IF;

            BEGIN
                -- Does the server exists ?
                SELECT * INTO serversrow
                FROM public.servers AS s
                WHERE s.hostname = (cur->'hostname');

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on public.servers: %L - %L';
                    EXECUTE format('INSERT INTO public.servers(hostname) VALUES (%L) RETURNING *', (cur->'hostname')) INTO STRICT serversrow;
                END IF;

                -- Does the service exists ?
                SELECT s2.* INTO servicesrow
                FROM public.servers AS s1
                    JOIN wh_nagios.services AS s2 ON s1.id = s2.id_server
                WHERE s1.hostname = (cur->'hostname')
                    AND s2.service = (cur->'servicedesc');

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on wh_nagios.services: %L - %L';

                    INSERT INTO wh_nagios.services (id, id_server, warehouse, service, state)
                    VALUES (default, serversrow.id, 'wh_nagios', cur->'servicedesc', cur->'servicestate')
                    RETURNING * INTO STRICT servicesrow;

                    EXECUTE format('UPDATE wh_nagios.services
                        SET oldest_record = timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''
                        WHERE id = $1', (cur->'timet'))
                        USING servicesrow.id;
                END IF;

                -- Store services informations to update them once per batch
                msg_err := 'Error during service statistics collect: %L - %L';
                IF ( updates[0] IS NULL ) THEN
                    -- initialize arrays
                    updates[0] := hstore(servicesrow.id::text,cur->'timet');
                    updates[1] := hstore(servicesrow.id::text,cur->'servicestate');
                END IF;
                IF ( ( updates[0]->(servicesrow.id)::text ) IS NULL ) THEN
                    -- new service found in hstore
                    updates[0] := updates[0] || hstore(servicesrow.id::text,cur->'timet');
                    updates[1] := updates[1] || hstore(servicesrow.id::text,cur->'servicestate');
                ELSE
                    -- service exists in hstore
                    IF ( ( updates[0]->(servicesrow.id)::text )::bigint < (cur->'timet')::bigint ) THEN
                        -- update the timet and state to the latest values
                        updates[0] := updates[0] || hstore(servicesrow.id::text,cur->'timet');
                        updates[1] := updates[1] || hstore(servicesrow.id::text,cur->'servicestate');
                    END IF;
                END IF;

                -- Does the metric exists ? only create if it's real perfdata,
                -- not a " " label
                IF (cur->'label' != ' ') THEN
                    SELECT l.* INTO metricsrow
                    FROM wh_nagios.metrics AS l
                    WHERE id_service = servicesrow.id
                        AND label = (cur->'label');

                    IF NOT FOUND THEN
                        msg_err := 'Error during INSERT on wh_nagios.metrics: %L - %L';

                        -- The trigger on wh_nagios.services_metric will create the partition counters_detail_$service_id automatically
                        INSERT INTO wh_nagios.metrics (id_service, label, unit, min, max, warning, critical)
                        VALUES (servicesrow.id, cur->'label', cur->'uom', (cur->'min')::numeric, (cur->'max')::numeric, (cur->'warning')::numeric, (cur->'critical')::numeric)
                        RETURNING * INTO STRICT metricsrow;
                    END IF;

                    --Do we need to update the metric ?
                    IF ( ( (cur->'uom') IS NOT NULL AND (metricsrow.unit <> (cur->'uom') OR (metricsrow.unit IS NULL)) )
                        OR ( (cur->'min') IS NOT NULL AND (metricsrow.min <> (cur->'min')::numeric OR (metricsrow.min IS NULL)) )
                        OR ( (cur->'max') IS NOT NULL AND (metricsrow.max <> (cur->'max')::numeric OR (metricsrow.max IS NULL)) )
                        OR ( (cur->'warning') IS NOT NULL AND (metricsrow.warning <> (cur->'warning')::numeric OR (metricsrow.warning IS NULL)) )
                        OR ( (cur->'critical') IS NOT NULL AND (metricsrow.critical <> (cur->'critical')::numeric OR (metricsrow.critical IS NULL)) )
                    ) THEN
                        msg_err := 'Error during UPDATE on wh_nagios.metrics: %L - %L';

                        EXECUTE pg_catalog.format('UPDATE wh_nagios.metrics SET
                                unit = %L,
                                min = %L,
                                max = %L,
                                warning = %L,
                                critical = %L
                            WHERE id = $1',
                            cur->'uom',
                            cur->'min',
                            cur->'max',
                            cur->'warning',
                            cur->'critical'
                        ) USING metricsrow.id;
                    END IF;
                END IF;

                IF (servicesrow.id IS NOT NULL AND servicesrow.last_cleanup < now() - '10 days'::interval) THEN
                    PERFORM wh_nagios.cleanup_service(servicesrow.id);
                END IF;


                msg_err := pg_catalog.format('Error during INSERT on counters_detail_%s: %%L - %%L', metricsrow.id);

                -- Do we need to insert a value ? if label is " " then perfdata
                -- was empty
                IF (cur->'label' != ' ') THEN
                    EXECUTE pg_catalog.format(
                        'INSERT INTO wh_nagios.counters_detail_%s (date_records,records)
                        VALUES (
                            date_trunc(''day'',timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''),
                            array[row(timestamp with time zone ''epoch''+%L * INTERVAL ''1 second'',%L )]::public.metric_value[]
                        )',
                        metricsrow.id,
                        cur->'timet',
                        cur->'timet',
                        cur->'value'
                    );
                END IF;

                -- one line has been processed with success !
                processed := processed  + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    IF log_error THEN
                        INSERT INTO wh_nagios.hub_reject (id, rolname, data, msg) VALUES (r_hub.id, r_hub.rolname, r_hub.data, pg_catalog.format(msg_err, SQLSTATE, SQLERRM)) ;
                    END IF;

                    -- We fail on the way for this one
                    failed := failed + 1;
            END;

            --Delete current line (processed or failed)
            DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;
        END LOOP;

        --Update the services, if needed
        FOR r_hub IN SELECT * FROM each(updates[0]) LOOP
            EXECUTE pg_catalog.format('UPDATE wh_nagios.services SET last_modified = CURRENT_DATE,
              state = %L,
              newest_record = timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second''
              WHERE id = %s
              AND ( newest_record IS NULL OR newest_record < timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second'' )',
              updates[1]->r_hub.key,
              r_hub.value,
              r_hub.key,
              r_hub.value );
        END LOOP;
    EXCEPTION
        WHEN lock_not_available THEN
            --Have frendlier exception if concurrent function already running
            RAISE EXCEPTION 'Concurrent function already running.';
    END;
    RETURN;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.dispatch_record(integer, boolean) FROM public;

COMMENT ON FUNCTION wh_nagios.dispatch_record(integer, boolean) IS
'Parse and dispatch all rows in wh_nagios.hub into the good counters_detail_X partition.
If a row concerns a non-existent server, it will create it without owner, so that only admins can see it. If a row concerns a service that didn''t
had a cleanup for more than 10 days, it will perform a cleanup for it. If called with "true", it will log in the table "wh_nagios.hub_reject" all
rows that couldn''t be dispatched, with the exception message.';

SELECT * FROM public.register_api('wh_nagios.dispatch_record(integer, boolean)'::regprocedure);

-- Automatically create a new partition when a service is added.
CREATE OR REPLACE FUNCTION wh_nagios.create_partition_on_insert_metric()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE pg_catalog.format('CREATE TABLE wh_nagios.counters_detail_%s (date_records date, records public.metric_value[])', NEW.id);
    EXECUTE pg_catalog.format('CREATE INDEX ON wh_nagios.counters_detail_%s USING btree(date_records)', NEW.id);
    EXECUTE pg_catalog.format('REVOKE ALL ON TABLE wh_nagios.counters_detail_%s FROM public', NEW.id);

    RETURN NEW;
EXCEPTION
    WHEN duplicate_table THEN
        -- This can happen when restoring a logical backup, just ignore the
        -- error.
        RAISE LOG 'Table % already exists, continuing anyway',
            pg_catalog.format('wh_nagios.counters_detail_%s', NEW.id);
        RETURN NEW;
END;
$$;

COMMENT ON FUNCTION wh_nagios.create_partition_on_insert_metric() IS
'Trigger that create a dedicated partition when a new metric is inserted in the table wh_nagios.metrics,
and GRANT the necessary ACL on it.';

/* wh_nagios.cleanup_service(bigint)
Aggregate all data by day in an array, to avoid space overhead and benefit TOAST compression.
This will be done for every metric corresponding to the service.

@p_serviceid: ID of service to cleanup.
@return : true if everything went well.
*/
CREATE OR REPLACE
FUNCTION wh_nagios.cleanup_service(p_serviceid bigint)
RETURNS boolean
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
  v_servicefound boolean ;
  v_partid       bigint ;
  v_partname     text ;
BEGIN
    SELECT ( pg_catalog.count(1) = 1 ) INTO v_servicefound
    FROM wh_nagios.services AS s
    WHERE s.id = p_serviceid;

    IF NOT v_servicefound THEN
        RETURN false;
    END IF;

    -- Try to purge data before the cleanup
    PERFORM wh_nagios.purge_services(p_serviceid);

    FOR v_partid IN SELECT id FROM wh_nagios.metrics WHERE id_service = p_serviceid LOOP
        v_partname := pg_catalog.format('counters_detail_%s', v_partid);

        EXECUTE pg_catalog.format('CREATE TEMP TABLE tmp (LIKE wh_nagios.%I)', v_partname);

        EXECUTE pg_catalog.format('WITH list AS (SELECT date_records, pg_catalog.count(1) AS num
                FROM wh_nagios.%I
                GROUP BY date_records
            ),
            del AS (DELETE FROM wh_nagios.%I c
                USING list l WHERE c.date_records = l.date_records AND l.num > 1
                RETURNING c.*
            ),
            rec AS (SELECT date_records, (pg_catalog.unnest(records)).*
                FROM del
            )
            INSERT INTO tmp
            SELECT date_records, pg_catalog.array_agg(row(timet, value)::public.metric_value)
            FROM rec
            GROUP BY date_records
            UNION ALL
            SELECT cd.* FROM wh_nagios.%I cd JOIN list l USING (date_records)
            WHERE num = 1;
        ', v_partname, v_partname, v_partname, v_partname);
        EXECUTE pg_catalog.format('TRUNCATE wh_nagios.%I', v_partname);
        EXECUTE pg_catalog.format('INSERT INTO wh_nagios.%I SELECT * FROM tmp', v_partname);
        DROP TABLE tmp;
    END LOOP;

    UPDATE wh_nagios.services SET last_cleanup = pg_catalog.now() WHERE id = p_serviceid;

    RETURN true;
END
$$;



-- This line must be the last one, so that every functions are owned
-- by the database owner
SELECT * FROM public.set_extension_owner('wh_nagios');
