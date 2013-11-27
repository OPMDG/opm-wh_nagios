-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2013: Open PostgreSQL Monitoring Development Group

SET statement_timeout TO 0;

ALTER SCHEMA wh_nagios OWNER TO opm;
REVOKE ALL ON SCHEMA wh_nagios FROM public;
GRANT USAGE ON SCHEMA wh_nagios TO opm_roles;

CREATE TYPE wh_nagios.counters_detail AS (
    timet timestamp with time zone,
    value numeric
);
ALTER TYPE wh_nagios.counters_detail OWNER TO opm;

CREATE TABLE wh_nagios.hub (
    id bigserial,
    data text[]
);
ALTER TABLE wh_nagios.hub OWNER TO opm;
REVOKE ALL ON TABLE wh_nagios.hub FROM public;

CREATE TABLE wh_nagios.hub_reject (
    id bigserial,
    data text[],
    msg text
);
ALTER TABLE wh_nagios.hub_reject OWNER TO opm;
REVOKE ALL ON TABLE wh_nagios.hub_reject FROM public;

CREATE TABLE wh_nagios.services (
    state            text,
    oldest_record    timestamptz DEFAULT now(),
    newest_record    timestamptz
)
INHERITS (public.services);

ALTER TABLE wh_nagios.services OWNER TO opm;
ALTER TABLE wh_nagios.services ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_wh_nagios_services_id_server_service
    ON wh_nagios.services USING btree (id_server,service);
REVOKE ALL ON TABLE wh_nagios.services FROM public ;

CREATE TABLE wh_nagios.labels (
    id              bigserial PRIMARY KEY,
    id_service      bigint NOT NULL,
    label           text NOT NULL,
    unit            text,
    min             numeric,
    max             numeric,
    critical        numeric,
    warning         numeric
);
ALTER TABLE wh_nagios.labels OWNER TO opm;
REVOKE ALL ON wh_nagios.labels FROM public;
CREATE INDEX ON wh_nagios.labels USING btree (id_service);
ALTER TABLE wh_nagios.labels ADD CONSTRAINT wh_nagios_labels_fk
    FOREIGN KEY (id_service)
    REFERENCES wh_nagios.services (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE;

CREATE OR REPLACE VIEW wh_nagios.services_labels AS
    SELECT s.id, s.id_server, s.warehouse, s.service, s.last_modified,
        s.creation_ts, s.last_cleanup, s.servalid, s.state, l.min,
        l.max, l.critical, l.warning, s.oldest_record, s.newest_record,
        l.id as id_label, l.label, l.unit
    FROM wh_nagios.services s
    JOIN wh_nagios.labels l
        ON s.id = l.id_service;

ALTER VIEW wh_nagios.services_labels OWNER TO opm;
REVOKE ALL ON wh_nagios.services_labels FROM public;

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.services', '');
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.labels', '');
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.labels_id_seq', '');

/*
public.grant_service(service, role)

@return rc: status
 */
CREATE OR REPLACE FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
        v_state      text;
        v_msg        text;
        v_detail     text;
        v_hint       text;
        v_context    text;
        v_whname     text;
        v_label_id    bigint;
BEGIN
    FOR v_label_id IN (SELECT id_label FROM wh_nagios.list_label(p_service_id))
    LOOP
        EXECUTE format('GRANT SELECT ON wh_nagios.counters_detail_%s TO %I', v_label_id, p_rolname);
    END LOOP;
    rc := true;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        raise notice E'Unhandled error:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', v_state, v_msg, v_detail, v_hint, v_context;
        rc := false;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER;

ALTER FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO opm_admins;

COMMENT ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) IS 'Grant SELECT on a service.';

/*
public.revoke_service(service, role)

@return rc: status
 */
CREATE OR REPLACE FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
        v_state      text;
        v_msg        text;
        v_detail     text;
        v_hint       text;
        v_context    text;
        v_whname     text;
        v_label_id    bigint;
BEGIN
    FOR v_label_id IN (SELECT id_label FROM wh_nagios.list_label(p_service_id))
    LOOP
        EXECUTE format('REVOKE SELECT ON wh_nagios.counters_detail_%s FROM %I', v_label_id, p_rolname);
    END LOOP;
    rc := true;
EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        raise notice E'Unhandled error:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', v_state, v_msg, v_detail, v_hint, v_context;
        rc := false;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER;

ALTER FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO opm_admins;

COMMENT ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) IS 'Revoke SELECT on a service.';

/* wh_nagios.list_label(bigint)
Return every id and label for a service

@service_id: service wanted
@return : id and label for labels
*/
CREATE OR REPLACE FUNCTION wh_nagios.list_label(p_service_id bigint)
RETURNS TABLE (id_label bigint, label text, unit text, min numeric,
    max numeric, critical numeric, warning numeric)
AS $$
DECLARE
BEGIN
    IF is_admin(session_user) THEN
        RETURN QUERY SELECT l.id, l.label, l.unit, l.min, l.max, l.critical, l.warning
            FROM wh_nagios.labels l
            JOIN wh_nagios.services s
                ON s.id = l.id_service
            WHERE s.id = p_service_id;
    ELSE
        RETURN QUERY SELECT l.id, l.label, l.unit, l.min, l.max, l.critical, l.warning
            FROM list_services() s
            JOIN wh_nagios.labels l
                ON s.id = l.id_service
            WHERE s.id = p_service_id
        ;
        END IF;
END;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF
SECURITY DEFINER;

ALTER FUNCTION wh_nagios.list_label(bigint) OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.list_label(bigint) FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.list_label(bigint) TO opm_roles;

/* wh_nagios.list_services()
Return every wh_nagios.services%ROWTYPE

@return : wh_nagios.services%ROWTYPE
*/
CREATE OR REPLACE FUNCTION wh_nagios.list_services()
RETURNS SETOF wh_nagios.services
AS $$
DECLARE
BEGIN
    RETURN QUERY SELECT s2.*
        FROM public.list_servers() s1
        JOIN wh_nagios.services s2 ON s1.id = s2.id_server;
END;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF
SECURITY DEFINER;
ALTER FUNCTION wh_nagios.list_services() OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.list_services() FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.list_services() TO opm_roles;

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
    c_hub CURSOR FOR SELECT * FROM wh_nagios.hub LIMIT 50000 FOR UPDATE NOWAIT;
    r_hub record;
    i integer;
    cur hstore;
    msg_err text;
    servicesrow wh_nagios.services%ROWTYPE;
    labelsrow wh_nagios.labels%ROWTYPE;
    serversrow public.servers%ROWTYPE;
BEGIN
/*
TODO: Handle seracl
*/
    processed := 0;
    failed := 0;

    BEGIN
        FOR r_hub IN c_hub LOOP
            msg_err := NULL;

            --Check 1 dimension,at least 10 vals and even number of data
            IF ( (array_upper(r_hub.data, 2) IS NOT NULL) OR (array_upper(r_hub.data,1) < 10) OR ((array_upper(r_hub.data,1) % 2) <> 0) ) THEN
                IF (log_error = TRUE) THEN
                    msg_err := NULL;
                    IF (array_upper(r_hub.data,2) IS NOT NULL) THEN
                        msg_err := COALESCE(msg_err,'') || 'given array has more than 1 dimension';
                    END IF;
                    IF (array_upper(r_hub.data,1) <= 9) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'less than 10 values';
                    END IF;
                    IF ((array_upper(r_hub.data,1) % 2) != 0) THEN
                        msg_err := COALESCE(msg_err || ',','') || 'number of parameter not even';
                    END IF;

                    INSERT INTO wh_nagios.hub_reject (id, data,msg) VALUES (r_hub.id, r_hub.data, msg_err);
                END IF;

                failed := failed + 1;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;

                CONTINUE;
            END IF;

            cur := NULL;
            --Get all data as hstore,lowercase
            FOR i IN 1..array_upper(r_hub.data,1) BY 2 LOOP
                IF (cur IS NULL) THEN
                    cur := hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                ELSE
                    cur := cur || hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                END IF;
            END LOOP;

            serversrow := NULL;
            servicesrow := NULL;
            labelsrow := NULL;

            --Do we have all informations needed ?
            IF ( ((cur->'hostname') IS NULL)
                OR ((cur->'servicedesc') IS NULL)
                OR ((cur->'label') IS NULL)
                OR ((cur->'timet') IS NULL)
                OR ((cur->'value') IS NULL)
            ) THEN
                IF (log_error = TRUE) THEN
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

                    INSERT INTO wh_nagios.hub_reject (id, data,msg) VALUES (r_hub.id, r_hub.data, msg_err);
                END IF;

                failed := failed + 1;

                DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;

                CONTINUE;
            END IF;

            BEGIN
                --Does the server exists ?
                SELECT * INTO serversrow
                FROM public.servers
                WHERE hostname = (cur->'hostname');

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on public.servers: %L - %L';
                    EXECUTE format('INSERT INTO public.servers(hostname) VALUES (%L) RETURNING *', (cur->'hostname')) INTO STRICT serversrow;
                END IF;

                --Does the service exists ?
                SELECT s2.* INTO servicesrow
                FROM public.servers s1
                JOIN wh_nagios.services s2 ON s1.id = s2.id_server
                WHERE hostname = (cur->'hostname')
                    AND service = (cur->'servicedesc');

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT OR UPDATE on wh_nagios.services: %L - %L';

                    INSERT INTO wh_nagios.services (id,id_server,warehouse,service,state)
                    VALUES (default,serversrow.id,'wh_nagios',cur->'servicedesc',cur->'servicestate')
                    RETURNING * INTO STRICT servicesrow;
                    EXECUTE format('UPDATE wh_nagios.services
                        SET oldest_record = timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''
                        WHERE id = $1',(cur->'timet')) USING servicesrow.id;
                END IF;

                --Do we need to update the service ?
                IF ( (servicesrow.last_modified + '1 day'::interval < CURRENT_DATE)
                    OR ( (cur->'servicestate') IS NOT NULL AND (servicesrow.state <> (cur->'servicestate') OR servicesrow.state IS NULL) )
                    OR ( servicesrow.newest_record +'5 minutes'::interval < now() )
                ) THEN
                    msg_err := 'Error during UPDATE on wh_nagios.services: %L - %L';

                    EXECUTE format('UPDATE wh_nagios.services SET last_modified = CURRENT_DATE,
                            state = %L,
                            newest_record= timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second''
                        WHERE id = %s',
                        cur->'servicestate',
                        cur->'timet',
                        servicesrow.id);
                END IF;


                --Does the label exists ?
                SELECT l.* INTO labelsrow
                FROM wh_nagios.labels AS l
                WHERE id_service = servicesrow.id
                    AND label = (cur->'label');

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT on wh_nagios.labels: %L - %L';

                    -- The trigger on wh_nagios.services_label will create the partition counters_detail_$service_id automatically
                    INSERT INTO wh_nagios.labels (id_service, label, unit, min, max, warning, critical)
                    VALUES (servicesrow.id, cur->'label', cur->'uom', (cur->'min')::numeric, (cur->'max')::numeric, (cur->'warning')::numeric, (cur->'critical')::numeric)
                    RETURNING * INTO STRICT labelsrow;
                END IF;

                --Do we need to update the label ?
                IF ( ( (cur->'uom') IS NOT NULL AND (labelsrow.unit <> (cur->'uom') OR (labelsrow.unit IS NULL)) )
                    OR ( (cur->'min') IS NOT NULL AND (labelsrow.min <> (cur->'min')::numeric OR (labelsrow.min IS NULL)) )
                    OR ( (cur->'max') IS NOT NULL AND (labelsrow.max <> (cur->'max')::numeric OR (labelsrow.max IS NULL)) )
                    OR ( (cur->'warning') IS NOT NULL AND (labelsrow.warning <> (cur->'warning')::numeric OR (labelsrow.warning IS NULL)) )
                    OR ( (cur->'critical') IS NOT NULL AND (labelsrow.critical <> (cur->'critical')::numeric OR (labelsrow.critical IS NULL)) )
                ) THEN
                    msg_err := 'Error during UPDATE on wh_nagios.labels: %L - %L';

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
                    ) USING labelsrow.id;
                END IF;


                IF (servicesrow IS NOT NULL AND servicesrow.last_cleanup < now() - '10 days'::interval) THEN
                    PERFORM wh_nagios.cleanup_service(servicesrow.id,now()- '7 days'::interval);
                END IF;


                msg_err := format('Error during INSERT on counters_detail_%s: %%L - %%L', labelsrow.id);

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
                );

                -- one line has been processed with success !
                processed := processed  + 1;
            EXCEPTION
                WHEN OTHERS THEN
                    IF (log_error = TRUE) THEN
                        INSERT INTO wh_nagios.hub_reject (id, data,msg) VALUES (r_hub.id, r_hub.data, format(msg_err, SQLSTATE, SQLERRM));
                    END IF;

                    -- We fail on the way for this one
                    failed := failed + 1;
            END;

            --Delete current line (processed or failed)
            DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;
        END LOOP;
    EXCEPTION
        WHEN lock_not_available THEN
            --Have frendlier exception if concurrent function already running
            RAISE EXCEPTION 'Concurrent function already running.';
    END;
    RETURN;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF;

ALTER FUNCTION wh_nagios.dispatch_record(boolean)
    OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.dispatch_record(boolean)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.dispatch_record(boolean)
    TO opm_admins;

/* wh_nagios.cleanup_service(bigint,timestamptz)
Aggregate all data by day in an array, to avoid space overhead. It also delete consecutive rows with same value between the two bounds.
This will be done for every label corresponding to the service.

@p_serviceid: ID of service to cleanup.
@p_max_timestamp: Use to specify which rows to analyze for deleting. Will happen between last_cleanup and this parameter.
@return : true if everything went well.
*/
CREATE OR REPLACE FUNCTION cleanup_service(p_serviceid bigint, p_max_timestamp timestamp with time zone)
    RETURNS boolean
    AS $$
DECLARE
  c_tmp refcursor;
  r_tmp record;
  v_partid bigint;
  v_current_value numeric;
  v_start_range timestamptz;
  v_partname text;
  v_previous_timet timestamptz;
  v_counter integer;
  v_previous_cleanup timestamptz;
  v_cursor_found boolean;
  v_oldest timestamptz;
  v_newest timestamptz;
BEGIN
    SELECT last_cleanup INTO v_previous_cleanup FROM wh_nagios.services WHERE id = p_serviceid;

    IF NOT FOUND THEN
        RETURN false;
    END IF;
    FOR v_partid IN SELECT id FROM wh_nagios.labels WHERE id_service = p_serviceid LOOP
        v_partname := 'counters_detail_' || v_partid;

        EXECUTE 'LOCK TABLE wh_nagios.' || v_partname;
        EXECUTE 'CREATE TEMP TABLE tmp AS SELECT (unnest(records)).* FROM wh_nagios.'|| v_partname;

        SELECT min(timet),max(timet) INTO v_oldest,v_newest FROM tmp;

        OPEN c_tmp FOR EXECUTE 'SELECT timet,value FROM tmp WHERE timet >= ' || quote_literal(v_previous_cleanup) || ' AND timet <= ' || quote_literal(p_max_timestamp) || ' ORDER BY timet';
        LOOP
            FETCH c_tmp INTO r_tmp;
            v_cursor_found := FOUND;
            v_counter := v_counter+1;
            IF (v_cursor_found AND v_current_value IS NULL) THEN
                v_current_value := r_tmp.value;
                v_start_range := r_tmp.timet;
                v_counter := 1;
            ELSIF (NOT v_cursor_found OR v_current_value <> r_tmp.value) THEN
                IF (v_counter>= 4) THEN
                    RAISE DEBUG 'DELETE between % and % on partition %, counter=%',v_start_range,v_previous_timet,v_partid,v_counter;
                    EXECUTE 'DELETE FROM tmp WHERE timet > $1 AND timet < $2' USING v_start_range,v_previous_timet;
                END IF;
                EXIT WHEN NOT v_cursor_found;

                v_start_range := r_tmp.timet;
                v_current_value := r_tmp.value;
                v_counter := 1;
            END IF;
            v_previous_timet := r_tmp.timet;
        END LOOP;
        CLOSE c_tmp;

        RAISE DEBUG 'truncate wh_nagios.%',v_partname;
        EXECUTE 'TRUNCATE wh_nagios.' || v_partname;
        EXECUTE 'INSERT INTO wh_nagios.' || v_partname|| '
            SELECT date_trunc(''day'',timet),array_agg(row(timet,value)::wh_nagios.counters_detail)
            FROM tmp
            GROUP BY date_trunc(''day'',timet)';
        EXECUTE 'DROP TABLE tmp';
    END LOOP;

    UPDATE wh_nagios.services SET last_cleanup = p_max_timestamp, oldest_record = v_oldest, newest_record = v_newest
        WHERE id = p_serviceid;
    RETURN true;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF;

ALTER FUNCTION wh_nagios.cleanup_service(bigint, timestamp with time zone)
    OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.cleanup_service(bigint, timestamp with time zone)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.cleanup_service(bigint, timestamp with time zone)
    TO opm_admins;

CREATE FUNCTION wh_nagios.get_sampled_label_data(id_label bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
BEGIN
    IF (sample_sec > 0) THEN
        RETURN QUERY EXECUTE 'SELECT min(timet), max(value) FROM (SELECT (unnest(records)).* FROM wh_nagios.counters_detail_'||id_label||' where date_records >= $1 - ''1 day''::interval and date_records <= $2) as tmp WHERE timet >= $1 AND timet <= $2  group by (extract(epoch from timet)::float8/$3)::bigint*$3 ORDER BY 1' USING timet_begin,timet_end,sample_sec;
    ELSE
        RETURN QUERY EXECUTE 'SELECT min(timet), max(value) FROM (SELECT (unnest(records)).* FROM wh_nagios.counters_detail_'||id_label||' where date_records >= $1 - ''1 day''::interval and date_records <= $2) as tmp WHERE timet >= $1 AND timet <= $2 ORDER BY 1' USING timet_begin,timet_end;
    END IF;
END;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF;

ALTER FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    TO opm_roles;


CREATE FUNCTION wh_nagios.get_sampled_label_data(i_hostname text, i_service text, i_label text, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
DECLARE
    v_id_label bigint;
BEGIN
    SELECT id INTO v_id_label
    FROM wh_nagios.services_label
    WHERE hostname = i_hostname
        AND service = i_service
        AND label = i_label;

    IF NOT FOUND THEN
        RETURN;
    ELSE
        RETURN QUERY SELECT * FROM wh_nagios.get_sampled_label_data(v_id_label,timet_begin,timet_end,sample_sec);
    END IF;
END;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF;

ALTER FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    TO opm_roles;


/*
wh_nagios.grant_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE FUNCTION
wh_nagios.grant_dispatcher( IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
    v_state   TEXT;
    v_msg     TEXT;
    v_detail  TEXT;
    v_hint    TEXT;
    v_context TEXT;
BEGIN

    /* verify that the given role exists */
    rc := public.is_opm_role(p_rolname);

    IF NOT rc THEN
        /* this is OK to explicitly raise that the role does not exists
           as this function is granted to admins only anyway */
        RAISE WARNING 'Given role ''%'' is not an OPM role!', p_rolname;
        RETURN;
    END IF;

    EXECUTE format('GRANT USAGE ON SCHEMA wh_nagios TO %I', p_rolname);
    EXECUTE format('GRANT USAGE ON SEQUENCE wh_nagios.hub_id_seq TO %I', p_rolname);
    EXECUTE format('GRANT INSERT ON TABLE wh_nagios.hub TO %I', p_rolname);

    RAISE NOTICE 'GRANTED';

    rc := true;

    RETURN;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        raise WARNING 'Could not grant dispatch to ''%'' on wh_nagios:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', p_rolname, v_state, v_msg, v_detail, v_hint, v_context;

        rc := false;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER;

ALTER FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) TO opm_admins;

COMMENT ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean)
    IS 'Grant a role to dispatch performance data in warehouse wh_nagios.';

/*
wh_nagios.revoke_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE FUNCTION wh_nagios.revoke_dispatcher( IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
    v_state   TEXT;
    v_msg     TEXT;
    v_detail  TEXT;
    v_hint    TEXT;
    v_context TEXT;
BEGIN

    /* verify that the given role exists */
    rc := public.is_opm_role(p_rolname);

    IF NOT rc THEN
        /* this is OK to explicitly raise that the role does not exists
           as this function is granted to admins only anyway */
        RAISE WARNING 'Given role ''%'' is not an OPM role!', p_rolname;
        RETURN;
    END IF;

    EXECUTE format('REVOKE ALL ON SEQUENCE wh_nagios.hub_id_seq FROM %I', p_rolname);
    EXECUTE format('REVOKE ALL ON TABLE wh_nagios.hub FROM %I', p_rolname);

    RAISE NOTICE 'REVOKED';

    rc := true;

    RETURN;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT;
        raise WARNING 'Could not revoke dispatch to ''%'' on wh_nagios:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', p_rolname, v_state, v_msg, v_detail, v_hint, v_context;

        rc := false;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER;

ALTER FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) TO opm_admins;

COMMENT ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean)
    IS 'Revoke dispatch performance data from a role in wh_nagios.';


--Automatically create a new partition when a service is added.
CREATE OR REPLACE FUNCTION wh_nagios.create_partition_on_insert_label() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_rolname name;
BEGIN
    EXECUTE format('CREATE TABLE wh_nagios.counters_detail_%s (date_records date, records wh_nagios.counters_detail[])', NEW.id);
    EXECUTE format('ALTER TABLE wh_nagios.counters_detail_%s OWNER TO opm', NEW.id);
    EXECUTE format('REVOKE ALL ON TABLE wh_nagios.counters_detail_%s FROM public', NEW.id);

    SELECT rolname INTO v_rolname
    FROM public.list_servers() s1
    JOIN wh_nagios.services s2 ON s2.id_server = s1.id
    WHERE s2.id = NEW.id_service ;

    IF ( v_rolname IS NOT NULL) THEN
        EXECUTE format('GRANT SELECT ON TABLE wh_nagios.counters_detail_%s TO %I', NEW.id, v_rolname);
    END IF;

    RETURN NEW;
EXCEPTION
    WHEN duplicate_table THEN
        EXECUTE format('TRUNCATE TABLE wh_nagios.counters_detail_%s', NEW.id);
        RETURN NEW;
END;
$$;

ALTER FUNCTION wh_nagios.create_partition_on_insert_label() OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() FROM public;
GRANT ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() TO opm_admins;

--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_label()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DROP TABLE wh_nagios.counters_detail_%s', OLD.id);
    RETURN NULL;
EXCEPTION
    WHEN undefined_table THEN
        RETURN NULL;
END;
$$;

ALTER FUNCTION wh_nagios.drop_partition_on_delete_label() OWNER TO opm;
REVOKE ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() FROM public;
GRANT ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() TO opm_admins;

CREATE TRIGGER create_partition_on_insert_service
    BEFORE INSERT ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_label();
CREATE TRIGGER drop_partition_on_delete_service
    AFTER DELETE ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_label();
