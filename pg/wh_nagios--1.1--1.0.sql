-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group

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

DROP FUNCTION wh_nagios.dispatch_record(integer, boolean);

DROP FUNCTION wh_nagios.purge_services(VARIADIC bigint[]);

DROP FUNCTION IF EXISTS wh_nagios.delete_services(VARIADIC bigint[]);

DROP FUNCTION IF EXISTS wh_nagios.update_services_validity(interval, VARIADIC bigint[]);

DROP FUNCTION IF EXISTS wh_nagios.delete_labels(VARIADIC bigint[]);

/* wh_nagios.dispatch_record(boolean)
Dispatch records from wh_nagios.hub into counters_detail_$ID

$ID is found in wh_nagios.services_label and wh_nagios.services, with correct hostname,servicedesc and label

@log_error: If true, will report errors and details in wh_nagios.hub_reject
@return : true if everything went well.
*/
CREATE OR REPLACE FUNCTION wh_nagios.dispatch_record(log_error boolean DEFAULT false, OUT processed bigint, OUT failed bigint)
    AS $$
DECLARE
    --Select current lines and lock them so then can be deleted
    --Use NOWAIT so there can't be two concurrent processes
    c_hub CURSOR FOR SELECT * FROM wh_nagios.hub LIMIT 50000 FOR UPDATE NOWAIT ;
    r_hub record ;
    i integer ;
    cur hstore ;
    msg_err text ;
    servicesrow wh_nagios.services%ROWTYPE ;
    labelsrow wh_nagios.labels%ROWTYPE ;
    serversrow public.servers%ROWTYPE ;
    updates hstore[2] ;
BEGIN
/*
TODO: Handle seracl
*/
    processed := 0 ;
    failed := 0 ;

    BEGIN
        FOR r_hub IN c_hub LOOP
            msg_err := NULL ;

            --Check 1 dimension,at least 10 vals and even number of data
            IF ( (array_upper(r_hub.data, 2) IS NOT NULL) OR (array_upper(r_hub.data,1) < 10) OR ((array_upper(r_hub.data,1) % 2) <> 0) ) THEN
                IF (log_error = TRUE) THEN
                    msg_err := NULL ;
                    IF (array_upper(r_hub.data,2) IS NOT NULL) THEN
                        msg_err := COALESCE(msg_err,'') || 'given array has more than 1 dimension' ;
                    END IF ;
                    IF (array_upper(r_hub.data,1) <= 9) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'less than 10 values' ;
                    END IF ;
                    IF ((array_upper(r_hub.data,1) % 2) != 0) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'number of parameter not even' ;
                    END IF ;

                    INSERT INTO wh_nagios.hub_reject (id, rolname, data,msg) VALUES (r_hub.id, r_hub.rolname, r_hub.data, msg_err) ;
                END IF ;

                failed := failed + 1 ;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub ;

                CONTINUE ;
            END IF ;

            cur := NULL ;
            --Get all data as hstore,lowercase
            FOR i IN 1..array_upper(r_hub.data,1) BY 2 LOOP
                IF (cur IS NULL) THEN
                    cur := hstore(lower(r_hub.data[i]),r_hub.data[i+1]) ;
                ELSE
                    cur := cur || hstore(lower(r_hub.data[i]),r_hub.data[i+1]) ;
                END IF ;
            END LOOP ;

            serversrow := NULL ;
            servicesrow := NULL ;
            labelsrow := NULL ;

            --Do we have all informations needed ?
            IF ( ((cur->'hostname') IS NULL)
                OR ((cur->'servicedesc') IS NULL)
                OR ((cur->'label') IS NULL)
                OR ((cur->'timet') IS NULL)
                OR ((cur->'value') IS NULL)
            ) THEN
                IF (log_error = TRUE) THEN
                    msg_err := NULL ;
                    IF ((cur->'hostname') IS NULL) THEN
                        msg_err := COALESCE(msg_err,'') || 'hostname required' ;
                    END IF ;
                    IF ((cur->'servicedesc') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'servicedesc required' ;
                    END IF ;
                    IF ((cur->'label') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'label required' ;
                    END IF ;
                    IF ((cur->'timet') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'timet required' ;
                    END IF ;
                    IF ((cur->'value') IS NULL) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'value required' ;
                    END IF ;

                    INSERT INTO wh_nagios.hub_reject (id, rolname, data, msg) VALUES (r_hub.id, r_hub.rolname, r_hub.data, msg_err) ;
                END IF ;

                failed := failed + 1 ;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub ;

                CONTINUE ;
            END IF ;

            BEGIN
                --Does the server exists ?
                SELECT * INTO serversrow
                FROM public.servers
                WHERE hostname = (cur->'hostname') ;

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on public.servers: %L - %L' ;
                    EXECUTE format('INSERT INTO public.servers(hostname) VALUES (%L) RETURNING *', (cur->'hostname')) INTO STRICT serversrow ;
                END IF ;

                --Does the service exists ?
                SELECT s2.* INTO servicesrow
                FROM public.servers s1
                JOIN wh_nagios.services s2 ON s1.id = s2.id_server
                WHERE hostname = (cur->'hostname')
                    AND service = (cur->'servicedesc') ;

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on wh_nagios.services: %L - %L' ;

                    INSERT INTO wh_nagios.services (id,id_server,warehouse,service,state)
                    VALUES (default,serversrow.id,'wh_nagios',cur->'servicedesc',cur->'servicestate')
                    RETURNING * INTO STRICT servicesrow ;
                    EXECUTE format('UPDATE wh_nagios.services
                        SET oldest_record = timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''
                        WHERE id = $1',(cur->'timet')) USING servicesrow.id ;
                END IF ;

                -- Store services informations to update them once per batch
                msg_err := 'Error during service statistics collect: %L - %L' ;
                IF ( updates[0] IS NULL ) THEN
                    -- initialize arrays
                    updates[0] := hstore(servicesrow.id::text,cur->'timet') ;
                    updates[1] := hstore(servicesrow.id::text,cur->'servicestate') ;
                END IF;
                IF ( ( updates[0]->(servicesrow.id)::text ) IS NULL ) THEN
                    -- new service found in hstore
                    updates[0] := updates[0] || hstore(servicesrow.id::text,cur->'timet') ;
                    updates[1] := updates[1] || hstore(servicesrow.id::text,cur->'servicestate') ;
                ELSE
                    -- service exists in hstore
                    IF ( ( updates[0]->(servicesrow.id)::text )::bigint < (cur->'timet')::bigint ) THEN
                        -- update the timet and state to the latest values
                        updates[0] := updates[0] || hstore(servicesrow.id::text,cur->'timet') ;
                        updates[1] := updates[1] || hstore(servicesrow.id::text,cur->'servicestate') ;
                    END IF;
                END IF;

                --Does the label exists ?
                SELECT l.* INTO labelsrow
                FROM wh_nagios.labels AS l
                WHERE id_service = servicesrow.id
                    AND label = (cur->'label') ;

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT on wh_nagios.labels: %L - %L' ;

                    -- The trigger on wh_nagios.services_label will create the partition counters_detail_$service_id automatically
                    INSERT INTO wh_nagios.labels (id_service, label, unit, min, max, warning, critical)
                    VALUES (servicesrow.id, cur->'label', cur->'uom', (cur->'min')::numeric, (cur->'max')::numeric, (cur->'warning')::numeric, (cur->'critical')::numeric)
                    RETURNING * INTO STRICT labelsrow ;
                END IF ;

                --Do we need to update the label ?
                IF ( ( (cur->'uom') IS NOT NULL AND (labelsrow.unit <> (cur->'uom') OR (labelsrow.unit IS NULL)) )
                    OR ( (cur->'min') IS NOT NULL AND (labelsrow.min <> (cur->'min')::numeric OR (labelsrow.min IS NULL)) )
                    OR ( (cur->'max') IS NOT NULL AND (labelsrow.max <> (cur->'max')::numeric OR (labelsrow.max IS NULL)) )
                    OR ( (cur->'warning') IS NOT NULL AND (labelsrow.warning <> (cur->'warning')::numeric OR (labelsrow.warning IS NULL)) )
                    OR ( (cur->'critical') IS NOT NULL AND (labelsrow.critical <> (cur->'critical')::numeric OR (labelsrow.critical IS NULL)) )
                ) THEN
                    msg_err := 'Error during UPDATE on wh_nagios.labels: %L - %L' ;

                    EXECUTE format('UPDATE wh_nagios.labels SET
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
                    ) USING labelsrow.id ;
                END IF ;


                IF (servicesrow.id IS NOT NULL AND servicesrow.last_cleanup < now() - '10 days'::interval) THEN
                    PERFORM wh_nagios.cleanup_service(servicesrow.id) ;
                END IF ;


                msg_err := format('Error during INSERT on counters_detail_%s: %%L - %%L', labelsrow.id) ;

                EXECUTE format(
                    'INSERT INTO wh_nagios.counters_detail_%s (date_records,records)
                    VALUES (
                        date_trunc(''day'',timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''),
                        array[row(timestamp with time zone ''epoch''+%L * INTERVAL ''1 second'',%L )]::wh_nagios.counters_detail[]
                    )',
                    labelsrow.id,
                    cur->'timet',
                    cur->'timet',
                    cur->'value'
                ) ;

                -- one line has been processed with success !
                processed := processed  + 1 ;
            EXCEPTION
                WHEN OTHERS THEN
                    IF (log_error = TRUE) THEN
                        INSERT INTO wh_nagios.hub_reject (id, rolname, data, msg) VALUES (r_hub.id, r_hub.rolname, r_hub.data, format(msg_err, SQLSTATE, SQLERRM)) ;
                    END IF ;

                    -- We fail on the way for this one
                    failed := failed + 1 ;
            END ;

            --Delete current line (processed or failed)
            DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub ;
        END LOOP ;

        --Update the services, if needed
        FOR r_hub IN SELECT * FROM each(updates[0]) LOOP
            EXECUTE format('UPDATE wh_nagios.services SET last_modified = CURRENT_DATE,
              state = %L,
              newest_record = timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second''
              WHERE id = %s
              AND ( newest_record IS NULL OR newest_record < timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second'' )',
              updates[1]->r_hub.key,
              r_hub.value,
              r_hub.key,
              r_hub.value ) ;
        END LOOP;
    EXCEPTION
        WHEN lock_not_available THEN
            --Have frendlier exception if concurrent function already running
            RAISE EXCEPTION 'Concurrent function already running.' ;
    END ;
    RETURN ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.dispatch_record(boolean)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.dispatch_record(boolean)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.dispatch_record(boolean)
    TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.dispatch_record(boolean) IS 'Parse and dispatch all rows in wh_nagios.hub into the good counters_detail_X partition.
If a row concerns a non-existent server, it will create it without owner, so that only admins can see it. If a row concerns a service that didn''t
had a cleanup for more than 10 days, it will perform a cleanup for it. If called with "true", it will log in the table "wh_nagios.hub_reject" all
rows that couldn''t be dispatched, with the exception message.' ;

ALTER TRIGGER create_partition_on_insert_label ON wh_nagios.labels RENAME TO create_partition_on_insert_service ;
ALTER TRIGGER drop_partition_on_delete_label ON wh_nagios.labels RENAME TO drop_partition_on_delete_service ;
