-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

SET statement_timeout TO 0;

ALTER SCHEMA wh_nagios OWNER TO pgfactory;
GRANT USAGE ON SCHEMA wh_nagios TO pgf_roles;

CREATE TYPE wh_nagios.counters_detail AS (
    timet timestamp with time zone,
    value numeric
);
ALTER TYPE wh_nagios.counters_detail OWNER TO pgfactory;

CREATE TABLE wh_nagios.hub (
    id bigserial,
    data text[]
);
ALTER TABLE wh_nagios.hub OWNER TO pgfactory;
REVOKE ALL ON TABLE wh_nagios.hub FROM public;

CREATE TABLE wh_nagios.hub_reject (
    id bigserial,
    data text[],
    msg text
);
ALTER TABLE wh_nagios.hub_reject OWNER TO pgfactory;
REVOKE ALL ON TABLE wh_nagios.hub_reject FROM public;

CREATE TABLE wh_nagios.services (
    unit             text,
    state            text,
    min              numeric,
    max              numeric,
    critical         numeric,
    warning          numeric,
    oldest_record    timestamptz DEFAULT now(),
    newest_record    timestamptz
)
INHERITS (public.services);

ALTER TABLE wh_nagios.services OWNER TO pgfactory;
ALTER TABLE wh_nagios.services ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_wh_nagios_services_hostname
    ON wh_nagios.services USING btree (hostname, service);
REVOKE ALL ON TABLE wh_nagios.services FROM public ;

CREATE TABLE wh_nagios.labels (
    id              bigserial PRIMARY KEY,
    id_service		bigint NOT NULL,
    label           text NOT NULL
);
ALTER TABLE wh_nagios.labels OWNER TO pgfactory;
REVOKE ALL ON wh_nagios.labels FROM public;
CREATE INDEX ON wh_nagios.labels USING btree (id_service);
ALTER TABLE wh_nagios.labels ADD CONSTRAINT wh_nagios_labels_fk FOREIGN KEY (id_service) REFERENCES wh_nagios.services (id) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE;

CREATE VIEW wh_nagios.services_labels AS
    SELECT s.*,l.id as id_label,l.label
    FROM wh_nagios.services s
    JOIN wh_nagios.labels l
        ON s.id = l.id_service;
ALTER VIEW wh_nagios.services_labels OWNER TO pgfactory;
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

ALTER FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO pgf_admins;

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
        EXECUTE format('REVOKE SELECT ON wh_nagios.counters_detail_%s TO %I', v_label_id, p_rolname);
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

ALTER FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO pgf_admins;

COMMENT ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) IS 'Revoke SELECT on a service.';

/* wh_nagios.list_label(bigint)
Return every id and label for a service

@service_id: service wanted
@return : id and label for labels
*/
CREATE OR REPLACE FUNCTION wh_nagios.list_label(p_service_id bigint) RETURNS TABLE (id_label bigint, label text)
AS $$
DECLARE
BEGIN
    IF pg_has_role(session_user, 'pgf_admins', 'MEMBER') THEN
        RETURN QUERY SELECT l.id, l.label
            FROM wh_nagios.labels l
            JOIN wh_nagios.services s
                ON s.id = l.id_service
            WHERE s.id = p_service_id;
    ELSE
        RETURN QUERY EXECUTE format('WITH RECURSIVE
                v_roles AS (
                    SELECT pr.oid AS oid, r.rolname, ARRAY[r.rolname] AS roles
                      FROM public.roles r
                      JOIN pg_catalog.pg_roles pr ON (r.rolname = pr.rolname)
                     WHERE r.rolname = %L
                    UNION ALL
                    SELECT pa.oid, v.rolname, v.roles|| pa.rolname
                      FROM v_roles v
                      JOIN pg_auth_members am ON (am.member = v.oid)
                      JOIN pg_roles pa ON (am.roleid = pa.oid)
                     WHERE NOT pa.rolname::name = ANY(v.roles)
                ),
                acl AS (
                    SELECT l.id, l.label, (aclexplode(seracl)).*
                    FROM wh_nagios.services s
                    JOIN wh_nagios.labels l ON l.id_service = s.id
                    WHERE s.id = %s
                    AND array_length(seracl, 1) IS NOT NULL
                )
                SELECT id, label
                FROM acl
                WHERE grantee IN (SELECT oid FROM v_roles) AND privilege_type = %L',
            session_user,p_service_id,'SELECT'
        );
        END IF;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER;
ALTER FUNCTION wh_nagios.list_label(bigint) OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.list_label(bigint) FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.list_label(bigint) TO pgf_roles;

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
    c_hub CURSOR FOR SELECT * FROM wh_nagios.hub FOR UPDATE NOWAIT;
    r_hub record;
    v_service_id bigint;
    v_service_label_id bigint;
    i integer;
    cur hstore;
    msg_err text;
    servicesrow wh_nagios.services%ROWTYPE;
BEGIN
/*
TODO: Handle seracl 

*/
    processed := 0;
    failed := 0;

    BEGIN
        FOR r_hub IN c_hub LOOP
            msg_err := NULL;
            --Check 1 dimension,even number of data and at least 10 vals
            IF ((array_upper(r_hub.data,2) IS NULL) AND (array_upper(r_hub.data,1) > 9) AND ((array_upper(r_hub.data,1) % 2) = 0)) THEN
                cur := NULL;
                --Get all data as hstore,lowercase
                FOR i IN 1..array_upper(r_hub.data,1) BY 2 LOOP
                    IF (cur IS NULL) THEN
                        cur := hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                    ELSE
                        cur := cur || hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                    END IF;
                END LOOP;
                servicesrow := NULL;
                --Do we have all informations needed ?
                IF ( ((cur->'hostname') IS NOT NULL) AND ((cur->'servicedesc') IS NOT NULL) AND ((cur->'label') IS NOT NULL) AND ((cur->'timet') IS NOT NULL) AND ((cur->'value') IS NOT NULL) ) THEN
                    BEGIN
                        --Does the service exists ?
                        SELECT * INTO servicesrow
                        FROM wh_nagios.services
                        WHERE hostname = (cur->'hostname')
                            AND service = (cur->'servicedesc');

                        IF NOT FOUND THEN
                            msg_err := 'Error during INSERT OR UPDATE on wh_nagios.services: %L - %L';
raise notice '>>>insert';
                            INSERT INTO wh_nagios.services (id,hostname,warehouse,service,seracl,unit,state,min,max,critical,warning)
                            VALUES (default,cur->'hostname','wh_nagios',cur->'servicedesc','{}'::aclitem[],cur->'uom',cur->'servicestate',(cur->'min')::numeric,(cur->'max')::numeric,(cur->'critical')::numeric,(cur->'warning')::numeric)
                            RETURNING id INTO STRICT v_service_id;
                            EXECUTE format('UPDATE wh_nagios.services SET oldest_record = timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''',(cur->'timet'));
                        ELSE
                            --Do we need to update the service ?
                            IF ( (servicesrow.last_modified + '1 day'::interval < CURRENT_DATE)
                                OR ( (cur->'servicestate') IS NOT NULL AND (servicesrow.state <> (cur->'servicestate') OR servicesrow.state IS NULL) )
                                OR ( (cur->'min') IS NOT NULL AND (servicesrow.min <> (cur->'min')::numeric OR (servicesrow.min IS NULL)) ) 
                                OR ( (cur->'max') IS NOT NULL AND (servicesrow.max <> (cur->'max')::numeric OR (servicesrow.max IS NULL)) )
                                OR ( (cur->'warning') IS NOT NULL AND (servicesrow.warning <> (cur->'warning')::numeric OR (servicesrow.warning IS NULL)) )
                                OR ( (cur->'critical') IS NOT NULL AND (servicesrow.critical <> (cur->'critical')::numeric OR (servicesrow.critical IS NULL)) )
                                OR ( (cur->'unit') IS NOT NULL AND (servicesrow.unit <> (cur->'unit') OR (servicesrow.unit IS NULL)) )
                                OR (servicesrow.newest_record +'5 minutes'::interval < now() )
                            ) THEN
                                msg_err := 'Error during UPDATE on wh_nagios.services: %L - %L';

                                EXECUTE format('UPDATE wh_nagios.services SET last_modified = CURRENT_DATE,
                                        state = %L,
                                        min = %L,
                                        max = %L,
                                        warning = %L,
                                        critical = %L,
                                        unit = %L,
                                        newest_record= timestamp with time zone ''epoch'' +%L * INTERVAL ''1 second''
                                    WHERE id = %s',
                                    cur->'servicestate',
                                    cur->'min',
                                    cur->'max',
                                    cur->'warning',
                                    cur->'critical',
                                    cur->'unit',
                                    cur->'timet',
                                    servicesrow.id);
                            END IF;
                            v_service_id := servicesrow.id;
                        END IF;

                        --Does the label exists ?
                        SELECT id INTO v_service_label_id
                            FROM wh_nagios.labels
                            WHERE id_service = v_service_id
                            AND label = (cur->'label');

                        IF NOT FOUND THEN
                            msg_err := 'Error during INSERT on wh_nagios.labels: %L - %L';

                            -- The trigger on wh_nagios.services_label will create the partition counters_detail_$service_id automatically
                            INSERT INTO wh_nagios.labels (id_service,label)
                                VALUES (v_service_id,(cur->'label')) RETURNING id INTO v_service_label_id;
                        END IF;

                        IF (servicesrow IS NOT NULL AND servicesrow.last_cleanup < now() - '10 days'::interval) THEN
                            PERFORM wh_nagios.cleanup_partition(servicesrow.id,now()- '7 days'::interval);
                        END IF;

                        msg_err := format('Error during INSERT on counters_detail_%s: %%L - %%L', v_service_label_id);

                        EXECUTE format(
                            'INSERT INTO wh_nagios.counters_detail_%s (date_records,records)
                            VALUES (
                                date_trunc(''day'',timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''),
                                array[row(timestamp with time zone ''epoch''+%L * INTERVAL ''1 second'',%L )]::wh_nagios.counters_detail[]
                            )',
                            v_service_label_id,
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

                            -- We faile on the way for this one
                            failed := failed + 1;
                    END;
                ELSE
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
                END IF;
            ELSE
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
            END IF;

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
    OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.dispatch_record(boolean)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.dispatch_record(boolean)
    TO pgf_admins;

/* wh_nagios.cleanup_partition(bigint,timestamptz)
Aggregate all data by day in an array, to avoid space overhead. It also delete consecutive rows with same value between the two bounds.

@p_partid: ID of partition to cleanup.
@p_max_timestamp: Use to specify which rows to analyze for deleting. Will happen between last_cleanup and this parameter.
@return : true if everything went well.
*/
CREATE OR REPLACE FUNCTION cleanup_partition(p_partid bigint, p_max_timestamp timestamp with time zone)
    RETURNS boolean
    AS $$
DECLARE
  c_tmp refcursor;
  r_tmp record;
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
    SELECT last_cleanup INTO v_previous_cleanup FROM wh_nagios.services WHERE id = p_partid;

    IF NOT FOUND THEN
        RETURN false;
    END IF;
    v_partname := 'counters_detail_' || p_partid;

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
                RAISE DEBUG 'DELETE BETWEEN % and % on partition %, counter=%',v_start_range,v_previous_timet,p_partid,v_counter;
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
    EXECUTE 'INSERT INTO wh_nagios.' || v_partname || ' SELECT date_trunc(''day'',timet),array_agg(row(timet,value)::wh_nagios.counters_detail) FROM tmp GROUP BY date_trunc(''day'',timet)';
    EXECUTE 'DROP TABLE tmp';

    UPDATE wh_nagios.services SET last_cleanup = p_max_timestamp, oldest_record = v_oldest, newest_record = v_newest
        WHERE id = p_partid;
    RETURN true;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF;

ALTER FUNCTION wh_nagios.cleanup_partition(bigint, timestamp with time zone)
    OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.cleanup_partition(bigint, timestamp with time zone)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.cleanup_partition(bigint, timestamp with time zone)
    TO pgf_admins;

CREATE FUNCTION wh_nagios.get_sampled_service_data(id_label bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
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
VOLATILE
LEAKPROOF;

ALTER FUNCTION wh_nagios.get_sampled_service_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_service_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_service_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    TO pgf_roles;

CREATE FUNCTION wh_nagios.get_sampled_service_data(i_hostname text, i_service text, i_label text, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
    RETURNS TABLE(timet timestamp with time zone, value numeric)
    AS $$
DECLARE
    v_id_label bigint;
BEGIN
    SELECT id INTO v_id_label FROM wh_nagios.services_label
    WHERE hostname = i_hostname
        AND service = i_service
        AND label = i_label;
    IF NOT FOUND THEN
        RETURN;
    ELSE
        RETURN QUERY SELECT * FROM wh_nagios.get_sampled_service_data(v_id_label,timet_begin,timet_end,sample_sec);
    END IF;
END;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF;

ALTER FUNCTION wh_nagios.get_sampled_service_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_service_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    FROM public;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_service_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    TO pgf_roles;


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

    /* verify that the give role exists */
    rc := public.is_pgf_role(p_rolname);

    IF NOT rc THEN
        RAISE WARNING 'Given role ''%'' is not a PGFactory role!', p_rolname;
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

ALTER FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) TO pgf_admins;

COMMENT ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean)
    IS 'Grant a role to dispatch performance data in warehouse wh_nagios.';

/*
wh_nagios.revoke_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE FUNCTION
wh_nagios.revoke_dispatcher( IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
    v_state   TEXT;
    v_msg     TEXT;
    v_detail  TEXT;
    v_hint    TEXT;
    v_context TEXT;
BEGIN

    /* verify that the give role exists */
    rc := public.is_pgf_role(p_rolname);

    IF NOT rc THEN
        RAISE WARNING 'Given role ''%'' is not a PGFactory role!', p_rolname;
        RETURN;
    END IF;

    EXECUTE format('REVOKE USAGE ON SCHEMA wh_nagios FROM %I', p_rolname);
    EXECUTE format('REVOKE USAGE ON SEQUENCE wh_nagios.hub_id_seq FROM %I', p_rolname);
    EXECUTE format('REVOKE INSERT ON TABLE wh_nagios.hub FROM %I', p_rolname);

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

ALTER FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) FROM public;
GRANT ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) TO pgf_admins;

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
    EXECUTE format('ALTER TABLE wh_nagios.counters_detail_%s OWNER TO pgfactory;', NEW.id);
    EXECUTE format('REVOKE ALL ON TABLE wh_nagios.counters_detail_%s FROM public;', NEW.id);
    
    FOR v_rolname IN (SELECT r.rolname FROM (SELECT (aclexplode(seracl)).grantee FROM public.services WHERE array_length(seracl, 1) IS NOT NULL) s JOIN pg_catalog.pg_roles r ON s.grantee = r.oid)
    LOOP
        EXECUTE format('GRANT SELECT ON TABLE wh_nagios.counters_detail_%s TO %I;', NEW.id, v_rolname);
    END LOOP;
    RETURN NEW;
EXCEPTION
    WHEN duplicate_table THEN
        EXECUTE format('TRUNCATE TABLE wh_nagios.counters_detail_%s', NEW.id);
        RETURN NEW;
END;
$$;

ALTER FUNCTION wh_nagios.create_partition_on_insert_label() OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() FROM public;
GRANT ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() TO pgf_admins;

--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_label() RETURNS trigger
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

ALTER FUNCTION wh_nagios.drop_partition_on_delete_label() OWNER TO pgfactory;
REVOKE ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() FROM public;
GRANT ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() TO pgf_admins;

CREATE TRIGGER create_partition_on_insert_service
    BEFORE INSERT ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_label();
CREATE TRIGGER drop_partition_on_delete_service
    AFTER DELETE ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_label();
