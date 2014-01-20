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

/* wh_nagios.purge_services(VARIADIC bigint[])
Delete records older than max(date_records) - service.servalid. Doesn't delete
any data if servalid IS NULL

@p_serviceid: ID's of service to purge. All services if null.
@return : number of services purged.
*/
CREATE OR REPLACE FUNCTION purge_services(VARIADIC p_servicesid bigint[] = NULL)
    RETURNS bigint
    AS $$
DECLARE
  i bigint ;
  v_allservices bigint[];
  v_serviceid bigint;
  v_servicefound boolean ;
  v_partid bigint ;
  v_partname text ;
  v_servalid interval;
  v_ret bigint;
  v_oldest timestamptz;
  v_oldtmp timestamptz;
BEGIN
    v_ret := 0 ;
    IF ( p_servicesid IS NULL ) THEN
        SELECT array_agg(id) INTO v_allservices FROM wh_nagios.services WHERE servalid IS NOT NULL;
    ELSE
        v_allservices := p_servicesid;
    END IF;

    IF ( v_allservices IS NULL ) THEN
        return v_ret;
    END IF;

    FOR i IN 1..array_upper(v_allservices, 1) LOOP
        v_serviceid := v_allservices[i];
        SELECT ( COUNT(*) = 1 ) INTO v_servicefound FROM wh_nagios.services WHERE id = v_serviceid AND servalid IS NOT NULL  ;
        IF v_servicefound THEN
            v_ret := v_ret + 1 ;

            SELECT servalid INTO STRICT v_servalid FROM wh_nagios.services WHERE id = v_serviceid ;

            FOR v_partid IN SELECT id FROM wh_nagios.labels WHERE id_service = v_serviceid LOOP
                v_partname := format('counters_detail_%s', v_partid) ;

                EXECUTE format('WITH m as ( SELECT max(date_records) as max
                        FROM wh_nagios.%I
                        )

                    DELETE
                    FROM wh_nagios.%I c
                    USING m
                    WHERE age(m.max, c.date_records) >= %L::interval;
                ', v_partname, v_partname, v_servalid);
                EXECUTE format('SELECT min(timet)
                    FROM (
                      SELECT (unnest(records)).timet
                      FROM (
                        SELECT records
                        FROM wh_nagios.%I
                        ORDER BY date_records ASC
                        LIMIT 1
                      )s
                    )s2 ;', v_partname) INTO v_oldtmp;
                v_oldest := least(v_oldest, v_oldtmp);
            END LOOP ;
            IF ( v_oldest IS NOT NULL ) THEN
                EXECUTE format('UPDATE wh_nagios.services
                  SET oldest_record = %L
                  WHERE id = %s', v_oldest, v_serviceid);
            END IF;
        END IF ;
    END LOOP;

    RETURN v_ret ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.purge_services(VARIADIC bigint[])
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.purge_services(VARIADIC bigint[])
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.purge_services(VARIADIC bigint[])
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.purge_services(VARIADIC bigint[]) IS 'Delete data older than retention interval.
The age is calculated from newest_record, not server date.' ;

DROP FUNCTION wh_nagios.dispatch_record(boolean);


/* wh_nagios.dispatch_record(boolean, integer)
Dispatch records from wh_nagios.hub into counters_detail_$ID

$ID is found in wh_nagios.services_label and wh_nagios.services, with correct hostname,servicedesc and label

@log_error: If true, will report errors and details in wh_nagios.hub_reject
@return : true if everything went well.
*/
CREATE OR REPLACE FUNCTION wh_nagios.dispatch_record(num_lines integer DEFAULT 5000, log_error boolean DEFAULT false, OUT processed bigint, OUT failed bigint)
    AS $$
DECLARE
    --Select current lines and lock them so then can be deleted
    --Use NOWAIT so there can't be two concurrent processes
    c_hub CURSOR FOR SELECT * FROM wh_nagios.hub LIMIT num_lines FOR UPDATE NOWAIT ;
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

ALTER FUNCTION wh_nagios.dispatch_record(integer, boolean)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.dispatch_record(integer, boolean)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.dispatch_record(integer, boolean)
    TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.dispatch_record(integer, boolean) IS 'Parse and dispatch all rows in wh_nagios.hub into the good counters_detail_X partition.
If a row concerns a non-existent server, it will create it without owner, so that only admins can see it. If a row concerns a service that didn''t
had a cleanup for more than 10 days, it will perform a cleanup for it. If called with "true", it will log in the table "wh_nagios.hub_reject" all
rows that couldn''t be dispatched, with the exception message.' ;

/* wh_nagios.delete_services(VARIADIC bigint[])
Delete a specific service.

Foreign key will delete related labels, and trigger will drop related partitions.

@p_serviceid: Unique identifiers of the services to deletes.
@return : true if eveything went well.
*/
CREATE OR REPLACE function wh_nagios.delete_services(VARIADIC p_servicesid bigint[])
    RETURNS boolean
    AS $$
DECLARE
  v_state      text ;
  v_msg        text ;
  v_detail     text ;
  v_hint       text ;
  v_context    text ;
  v_servicesid text ;
BEGIN
    v_servicesid := array_to_string(p_servicesid, ',');
    EXECUTE format('DELETE FROM wh_nagios.services WHERE id IN ( %s ) ', v_servicesid ) ;
    RETURN true ;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_state   = RETURNED_SQLSTATE,
        v_msg     = MESSAGE_TEXT,
        v_detail  = PG_EXCEPTION_DETAIL,
        v_hint    = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT ;
    raise notice E'Unhandled error:
        state  : %
        message: %
        detail : %
        hint   : %
        context: %', v_state, v_msg, v_detail, v_hint, v_context ;
    return false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.delete_services(VARIADIC bigint[])
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[])
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[])
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[]) IS 'Delete a service.
All related labels will also be deleted, and the corresponding partitions
will be dropped.' ;

/* wh_nagios.update_services_validity(interval, VARIADIC bigint[])
Update data retention of a specific service.

This function will not call pruge_services(), so data will stay until a purge
is manually executed, or next purge cron job if it exists.

@p_validity: New interval.
@p_servicesid: Unique identifiers of the services to update.
@return : true if eveything went well.
*/
CREATE OR REPLACE function wh_nagios.update_services_validity(p_validity interval, VARIADIC p_servicesid bigint[])
    RETURNS boolean
    AS $$
DECLARE
  v_state        text ;
  v_msg          text ;
  v_detail       text ;
  v_hint         text ;
  v_context      text ;
  v_serviceid    bigint ;
  v_servicesid   text ;
BEGIN
    v_servicesid := array_to_string(p_servicesid, ',');
    EXECUTE format('UPDATE wh_nagios.services set servalid = %L WHERE id IN ( %s ) ', p_validity, v_servicesid ) ;
    RETURN true ;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_state   = RETURNED_SQLSTATE,
        v_msg     = MESSAGE_TEXT,
        v_detail  = PG_EXCEPTION_DETAIL,
        v_hint    = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT ;
    raise notice E'Unhandled error:
        state  : %
        message: %
        detail : %
        hint   : %
        context: %', v_state, v_msg, v_detail, v_hint, v_context ;
    return false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.update_services_validity(interval, bigint[])
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.update_services_validity(interval, bigint[])
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.update_services_validity(interval, bigint[])
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.update_services_validity(interval, bigint[]) IS 'Update validity
of some services. This function won''t automatically purge the related data.' ;

/* wh_nagios.delete_labels(VARIADIC bigint[])
Delete specific labels.

Tigger will drop related partitions.

@p_labelsid: Unique identifiers of the labels to delete.
@return : true if eveything went well.
*/
CREATE OR REPLACE function wh_nagios.delete_labels(VARIADIC p_labelsid bigint[])
    RETURNS boolean
    AS $$
DECLARE
  v_state      text ;
  v_msg        text ;
  v_detail     text ;
  v_hint       text ;
  v_context    text ;
  v_labelsid text ;
BEGIN
    v_labelsid := array_to_string(p_labelsid, ',');
    EXECUTE format('DELETE FROM wh_nagios.labels WHERE id IN ( %s ) ', v_labelsid ) ;
    RETURN true ;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_state   = RETURNED_SQLSTATE,
        v_msg     = MESSAGE_TEXT,
        v_detail  = PG_EXCEPTION_DETAIL,
        v_hint    = PG_EXCEPTION_HINT,
        v_context = PG_EXCEPTION_CONTEXT ;
    raise notice E'Unhandled error:
        state  : %
        message: %
        detail : %
        hint   : %
        context: %', v_state, v_msg, v_detail, v_hint, v_context ;
    return false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.delete_labels(VARIADIC bigint[])
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.delete_labels(VARIADIC bigint[])
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.delete_labels(VARIADIC bigint[])
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.delete_labels(VARIADIC bigint[]) IS 'Delete labels.
The corresponding partitions will be dropped.' ;
