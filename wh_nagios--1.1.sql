-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group

SET statement_timeout TO 0 ;

ALTER SCHEMA wh_nagios OWNER TO opm ;
REVOKE ALL ON SCHEMA wh_nagios FROM public ;
GRANT USAGE ON SCHEMA wh_nagios TO opm_roles ;

CREATE TYPE wh_nagios.counters_detail AS (
    timet timestamp with time zone,
    value numeric
) ;
ALTER TYPE wh_nagios.counters_detail OWNER TO opm ;
COMMENT ON TYPE wh_nagios.counters_detail IS 'Composite type to store timestamped
values from services perdata. Thoses values will be stored as array in partition tables.' ;
COMMENT ON COLUMN wh_nagios.counters_detail.timet IS 'Timestamp of perfdata' ;
COMMENT ON COLUMN wh_nagios.counters_detail.value IS 'Value of perfdata' ;

CREATE TABLE wh_nagios.hub (
    id bigserial,
    rolname name NOT NULL default current_user,
    data text[]
) ;
ALTER TABLE wh_nagios.hub OWNER TO opm ;
REVOKE ALL ON TABLE wh_nagios.hub FROM public ;
COMMENT ON TABLE wh_nagios.hub IS 'Store raw perfdata from dispatchers. Those
data will be processed asynchronously by stored function wh_nagios.dispatch_record().
This table doesn''t have a primary key.' ;
COMMENT ON COLUMN wh_nagios.hub.id IS 'Batch identifier of the data importation.' ;
COMMENT ON COLUMN wh_nagios.hub.rolname IS 'User who inserted data.' ;
COMMENT ON COLUMN wh_nagios.hub.data IS 'Raw data as sent by dispatchers.' ;


CREATE TABLE wh_nagios.hub_reject (
    id bigserial NOT NULL,
    rolname name NOT NULL,
    data text[],
    msg text
) ;
ALTER TABLE wh_nagios.hub_reject OWNER TO opm ;
REVOKE ALL ON TABLE wh_nagios.hub_reject FROM public ;
COMMENT ON TABLE wh_nagios.hub_reject IS 'Store hub lines rejected by the
stored function wh_nagios.dispatch_record(), if it''s asked to log them.
This table doesn''t have a primary key.' ;
COMMENT ON COLUMN wh_nagios.hub_reject.id IS 'Batch identifier of failed data importation.' ;
COMMENT ON COLUMN wh_nagios.hub_reject.rolname IS 'User who inserted failed data.' ;
COMMENT ON COLUMN wh_nagios.hub_reject.data IS 'Raw data as sent by dispatchers.' ;
COMMENT ON COLUMN wh_nagios.hub_reject.msg IS 'Error message sent from wh_nagios.dispatch_record().' ;

CREATE TABLE wh_nagios.services (
    state            text,
    oldest_record    timestamptz NOT NULL DEFAULT now(),
    newest_record    timestamptz
)
INHERITS (public.services) ;

ALTER TABLE wh_nagios.services OWNER TO opm ;
ALTER TABLE wh_nagios.services ADD PRIMARY KEY (id) ;
ALTER TABLE wh_nagios.services ADD CONSTRAINT wh_nagios_services_id_server_fk
    FOREIGN KEY (id_server)
    REFERENCES public.servers (id) ON UPDATE CASCADE ON DELETE CASCADE ;
CREATE UNIQUE INDEX idx_wh_nagios_services_id_server_service
    ON wh_nagios.services USING btree (id_server,service) ;
REVOKE ALL ON TABLE wh_nagios.services FROM public ;
COMMENT ON TABLE wh_nagios.services IS 'Lists all available metrics for warehouse "wh_nagios". It''s inherited from the table public.services.' ;
COMMENT ON COLUMN wh_nagios.services.id IS 'Service unique identifier. Is the primary key.' ;
COMMENT ON COLUMN wh_nagios.services.id_server IS 'Identifier of the server.' ;
COMMENT ON COLUMN wh_nagios.services.warehouse IS 'warehouse that stores this specific metric. Fixed value of "wh_nagios" for this partition.' ;
COMMENT ON COLUMN wh_nagios.services.service IS 'service name that provides a specific metric.' ;
COMMENT ON COLUMN wh_nagios.services.last_modified IS 'last day that the dispatcher pushed datas in the warehouse.' ;
COMMENT ON COLUMN wh_nagios.services.creation_ts IS 'warehouse creation date and time for this particular service.' ;
COMMENT ON COLUMN wh_nagios.services.last_cleanup IS 'Last launch of "warehouse".cleanup_service(). Each warehouse has to implement his own, if needed.' ;
COMMENT ON COLUMN wh_nagios.services.servalid IS 'data retention time.' ;
COMMENT ON COLUMN wh_nagios.services.state IS 'Current nagios state of the service
(OK,WARNING,CRITICAL or UNKNOWN). This state is not timestamped.' ;
COMMENT ON COLUMN wh_nagios.services.oldest_record IS 'Timestamp of the oldest value stored for the service.' ;
COMMENT ON COLUMN wh_nagios.services.newest_record IS 'Timestamp of the newest value stored for the service.' ;

CREATE TABLE wh_nagios.labels (
    id              bigserial PRIMARY KEY,
    id_service      bigint NOT NULL,
    label           text NOT NULL,
    unit            text,
    min             numeric,
    max             numeric,
    critical        numeric,
    warning         numeric
) ;
ALTER TABLE wh_nagios.labels OWNER TO opm ;
REVOKE ALL ON wh_nagios.labels FROM public ;
CREATE INDEX ON wh_nagios.labels USING btree (id_service) ;
ALTER TABLE wh_nagios.labels ADD CONSTRAINT wh_nagios_labels_fk
    FOREIGN KEY (id_service)
    REFERENCES wh_nagios.services (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE ;
COMMENT ON TABLE wh_nagios.labels IS 'Stores all labels from services.' ;
COMMENT ON COLUMN wh_nagios.labels.id IS 'Label unique identifier. Is the primary key of table wh_nagios.labels' ;
COMMENT ON COLUMN wh_nagios.labels.id_service IS 'Referenced service in wh_nagios.' ;
COMMENT ON COLUMN wh_nagios.labels.label IS 'Title of label.' ;
COMMENT ON COLUMN wh_nagios.labels.unit IS 'Unit of the label.' ;
COMMENT ON COLUMN wh_nagios.labels.min IS 'Min value for the label.' ;
COMMENT ON COLUMN wh_nagios.labels.max IS 'Max value for the label.' ;
COMMENT ON COLUMN wh_nagios.labels.critical IS 'Critical threshold for the label.' ;
COMMENT ON COLUMN wh_nagios.labels.warning IS 'Warning threshold for the label.' ;

CREATE OR REPLACE VIEW wh_nagios.services_labels AS
    SELECT s.id, s.id_server, s.warehouse, s.service, s.last_modified,
        s.creation_ts, s.last_cleanup, s.servalid, s.state, l.min,
        l.max, l.critical, l.warning, s.oldest_record, s.newest_record,
        l.id as id_label, l.label, l.unit
    FROM wh_nagios.services s
    JOIN wh_nagios.labels l
        ON s.id = l.id_service ;

ALTER VIEW wh_nagios.services_labels OWNER TO opm ;
REVOKE ALL ON wh_nagios.services_labels FROM public ;
COMMENT ON VIEW wh_nagios.services_labels IS 'All informations for all services, and labels
if the service has labels.' ;
COMMENT ON COLUMN public.services.id IS 'Service unique identifier' ;
COMMENT ON COLUMN public.services.id_server IS 'Identifier of the server' ;
COMMENT ON COLUMN public.services.warehouse IS 'warehouse that stores this specific metric' ;
COMMENT ON COLUMN public.services.service IS 'service name that provides a specific metric' ;
COMMENT ON COLUMN public.services.last_modified IS 'last day that the dispatcher pushed datas in the warehouse' ;
COMMENT ON COLUMN public.services.creation_ts IS 'warehouse creation date and time for this particular service' ;
COMMENT ON COLUMN public.services.last_cleanup IS 'Last launch of "specific-warehouse".cleanup_service(). Each warehouse has to implement his own, if needed.' ;
COMMENT ON COLUMN public.services.servalid IS 'data retention time' ;
COMMENT ON COLUMN wh_nagios.services_labels.state IS 'Current nagios state of the service
(OK,WARNING,CRITICAL or UNKNOWN). This state is not timestamped.' ;
COMMENT ON COLUMN wh_nagios.services_labels.min IS 'Min value for the label.' ;
COMMENT ON COLUMN wh_nagios.services_labels.max IS 'Max value for the label.' ;
COMMENT ON COLUMN wh_nagios.services_labels.critical IS 'Critical threshold for the label.' ;
COMMENT ON COLUMN wh_nagios.services_labels.warning IS 'Warning threshold for the label.' ;
COMMENT ON COLUMN wh_nagios.services_labels.oldest_record IS 'Timestamp of the oldest value stored for the service.' ;
COMMENT ON COLUMN wh_nagios.services_labels.newest_record IS 'Timestamp of the newest value stored for the service.' ;
COMMENT ON COLUMN wh_nagios.services_labels.id_label IS 'Label unique identifier. Is the primary key of table wh_nagios.labels' ;
COMMENT ON COLUMN wh_nagios.services_labels.label IS 'Title of label.' ;
COMMENT ON COLUMN wh_nagios.services_labels.unit IS 'Unit of the label.' ;

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.services', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.labels', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.labels_id_seq', '');

/*
public.grant_service(service, role)

@return rc: status
 */
CREATE OR REPLACE FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
        v_state      text ;
        v_msg        text ;
        v_detail     text ;
        v_hint       text ;
        v_context    text ;
        v_whname     text ;
        v_label_id    bigint ;
BEGIN
    FOR v_label_id IN (SELECT id_label FROM wh_nagios.list_label(p_service_id))
    LOOP
        EXECUTE format('GRANT SELECT ON wh_nagios.counters_detail_%s TO %I', v_label_id, p_rolname) ;
    END LOOP ;
    rc := true ;
EXCEPTION
    WHEN OTHERS THEN
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
        rc := false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public ;
GRANT ALL ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.grant_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) IS 'Grant SELECT on a service, i.e. all its counters_detail_X partitions.' ;

/*
public.revoke_service(service, role)

@return rc: status
 */
CREATE OR REPLACE FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
        v_state      text ;
        v_msg        text ;
        v_detail     text ;
        v_hint       text ;
        v_context    text ;
        v_whname     text ;
        v_label_id    bigint ;
BEGIN
    FOR v_label_id IN (SELECT id_label FROM wh_nagios.list_label(p_service_id))
    LOOP
        EXECUTE format('REVOKE SELECT ON wh_nagios.counters_detail_%s FROM %I', v_label_id, p_rolname) ;
    END LOOP ;
    rc := true ;
EXCEPTION
    WHEN OTHERS THEN
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
        rc := false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) FROM public ;
GRANT ALL ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.revoke_service(IN p_service_id bigint, IN p_rolname name, OUT rc boolean) IS 'Revoke SELECT on a service, i.e. all its counters_detail_X partitions.' ;

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
            WHERE s.id = p_service_id ;
    ELSE
        RETURN QUERY SELECT l.id, l.label, l.unit, l.min, l.max, l.critical, l.warning
            FROM list_services() s
            JOIN wh_nagios.labels l
                ON s.id = l.id_service
            WHERE s.id = p_service_id
        ;
        END IF ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.list_label(bigint) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.list_label(bigint) FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.list_label(bigint) TO opm_roles ;

COMMENT ON FUNCTION wh_nagios.list_label(bigint) IS 'Return all labels for a specific service, if user is allowed to.' ;

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
        JOIN wh_nagios.services s2 ON s1.id = s2.id_server ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF
SECURITY DEFINER ;
ALTER FUNCTION wh_nagios.list_services() OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.list_services() FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.list_services() TO opm_roles ;

COMMENT ON FUNCTION wh_nagios.list_services() IS 'Return all services a user is allowed to see.' ;

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

/* wh_nagios.cleanup_service(bigint)
Aggregate all data by day in an array, to avoid space overhead and benefit TOAST compression.
This will be done for every label corresponding to the service.

@p_serviceid: ID of service to cleanup.
@return : true if everything went well.
*/
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

ALTER FUNCTION wh_nagios.cleanup_service(bigint)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.cleanup_service(bigint)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.cleanup_service(bigint)
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.cleanup_service(bigint) IS 'Aggregate all data by day in an array, to avoid space overhead and benefit TOAST compression.
This will be done for every label corresponding to the service.' ;


CREATE FUNCTION wh_nagios.get_sampled_label_data(id_label bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
BEGIN
    IF (sample_sec > 0) THEN
        RETURN QUERY EXECUTE format('SELECT min(timet), max(value) FROM (SELECT (unnest(records)).* FROM wh_nagios.counters_detail_%s where date_records >= $1 - ''1 day''::interval and date_records <= $2) as tmp WHERE timet >= $1 AND timet <= $2  group by (extract(epoch from timet)::float8/$3)::bigint*$3 ORDER BY 1', id_label) USING timet_begin,timet_end,sample_sec ;
    ELSE
        RETURN QUERY EXECUTE format('SELECT min(timet), max(value) FROM (SELECT (unnest(records)).* FROM wh_nagios.counters_detail_%s where date_records >= $1 - ''1 day''::interval and date_records <= $2) as tmp WHERE timet >= $1 AND timet <= $2 ORDER BY 1', id_label) USING timet_begin,timet_end ;
    END IF ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer)
    TO opm_roles ;
COMMENT ON FUNCTION wh_nagios.get_sampled_label_data(bigint, timestamp with time zone, timestamp with time zone, integer) IS 'Return sampled label data for the specified label unique identifier with the specified sampling interval (in second).' ;


CREATE FUNCTION wh_nagios.get_sampled_label_data(i_hostname text, i_service text, i_label text, timet_begin timestamp with time zone, timet_end timestamp with time zone, sample_sec integer)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
DECLARE
    v_id_label bigint ;
BEGIN
    SELECT id INTO v_id_label
    FROM wh_nagios.services_label
    WHERE hostname = i_hostname
        AND service = i_service
        AND label = i_label ;

    IF NOT FOUND THEN
        RETURN ;
    ELSE
        RETURN QUERY SELECT * FROM wh_nagios.get_sampled_label_data(v_id_label,timet_begin,timet_end,sample_sec) ;
    END IF ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer)
    TO opm_roles ;
COMMENT ON FUNCTION wh_nagios.get_sampled_label_data(text, text, text, timestamp with time zone, timestamp with time zone, integer) IS 'Return sampled label data for the specified hostname, service and label (all by name) with the specified sampling interval (in second).' ;


/*
wh_nagios.grant_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE FUNCTION
wh_nagios.grant_dispatcher( IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
    v_state   TEXT ;
    v_msg     TEXT ;
    v_detail  TEXT ;
    v_hint    TEXT ;
    v_context TEXT ;
BEGIN

    /* verify that the given role exists */
    rc := public.is_opm_role(p_rolname) ;

    IF NOT rc THEN
        /* this is OK to explicitly raise that the role does not exists
           as this function is granted to admins only anyway */
        RAISE WARNING 'Given role ''%'' is not an OPM role!', p_rolname ;
        RETURN ;
    END IF ;

    EXECUTE format('GRANT USAGE ON SCHEMA wh_nagios TO %I', p_rolname) ;
    EXECUTE format('GRANT USAGE ON SEQUENCE wh_nagios.hub_id_seq TO %I', p_rolname) ;
    EXECUTE format('GRANT INSERT ON TABLE wh_nagios.hub TO %I', p_rolname) ;

    RAISE NOTICE 'GRANTED' ;

    rc := true ;

    RETURN ;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT ;
        raise WARNING 'Could not grant dispatch to ''%'' on wh_nagios:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', p_rolname, v_state, v_msg, v_detail, v_hint, v_context ;

        rc := false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) FROM public ;
GRANT ALL ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean) TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.grant_dispatcher(IN name, OUT boolean)
    IS 'Grant a role to dispatch performance data in warehouse wh_nagios.' ;

/*
wh_nagios.revoke_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE FUNCTION wh_nagios.revoke_dispatcher( IN p_rolname name, OUT rc boolean)
AS $$
DECLARE
    v_state   TEXT ;
    v_msg     TEXT ;
    v_detail  TEXT ;
    v_hint    TEXT ;
    v_context TEXT ;
BEGIN

    /* verify that the given role exists */
    rc := public.is_opm_role(p_rolname) ;

    IF NOT rc THEN
        /* this is OK to explicitly raise that the role does not exists
           as this function is granted to admins only anyway */
        RAISE WARNING 'Given role ''%'' is not an OPM role!', p_rolname ;
        RETURN ;
    END IF ;

    EXECUTE format('REVOKE ALL ON SEQUENCE wh_nagios.hub_id_seq FROM %I', p_rolname) ;
    EXECUTE format('REVOKE ALL ON TABLE wh_nagios.hub FROM %I', p_rolname) ;

    RAISE NOTICE 'REVOKED' ;

    rc := true ;

    RETURN ;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state   = RETURNED_SQLSTATE,
            v_msg     = MESSAGE_TEXT,
            v_detail  = PG_EXCEPTION_DETAIL,
            v_hint    = PG_EXCEPTION_HINT,
            v_context = PG_EXCEPTION_CONTEXT ;
        raise WARNING 'Could not revoke dispatch to ''%'' on wh_nagios:
            state  : %
            message: %
            detail : %
            hint   : %
            context: %', p_rolname, v_state, v_msg, v_detail, v_hint, v_context ;

        rc := false ;
END ;
$$
LANGUAGE plpgsql
VOLATILE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) FROM public ;
GRANT ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) TO opm_admins ;

COMMENT ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean)
    IS 'Revoke dispatch performance data from a role in wh_nagios.' ;


--Automatically create a new partition when a service is added.
CREATE OR REPLACE FUNCTION wh_nagios.create_partition_on_insert_label() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_rolname name ;
BEGIN
    EXECUTE format('CREATE TABLE wh_nagios.counters_detail_%s (date_records date, records wh_nagios.counters_detail[])', NEW.id) ;
    EXECUTE format('CREATE INDEX ON wh_nagios.counters_detail_%s USING btree(date_records)', NEW.id) ;
    EXECUTE format('ALTER TABLE wh_nagios.counters_detail_%s OWNER TO opm', NEW.id) ;
    EXECUTE format('REVOKE ALL ON TABLE wh_nagios.counters_detail_%s FROM public', NEW.id) ;

    SELECT rolname INTO v_rolname
    FROM public.list_servers() s1
    JOIN wh_nagios.services s2 ON s2.id_server = s1.id
    WHERE s2.id = NEW.id_service ;

    IF ( v_rolname IS NOT NULL) THEN
        EXECUTE format('GRANT SELECT ON TABLE wh_nagios.counters_detail_%s TO %I', NEW.id, v_rolname) ;
    END IF ;

    RETURN NEW ;
EXCEPTION
    WHEN duplicate_table THEN
        EXECUTE format('TRUNCATE TABLE wh_nagios.counters_detail_%s', NEW.id) ;
        RETURN NEW ;
END ;
$$ ;

ALTER FUNCTION wh_nagios.create_partition_on_insert_label() OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() FROM public ;
GRANT ALL ON FUNCTION wh_nagios.create_partition_on_insert_label() TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.create_partition_on_insert_label() IS 'Trigger that create a dedicated partition when a new label is inserted in the table wh_nagios.labels,
and GRANT the necessary ACL on it.
If the dedicated partition is alreay present (which should not happen, due to the other trigger), it''s truncated.' ;

--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_label()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DROP TABLE wh_nagios.counters_detail_%s', OLD.id) ;
    RETURN NULL ;
EXCEPTION
    WHEN undefined_table THEN
        RETURN NULL ;
END ;
$$ ;

ALTER FUNCTION wh_nagios.drop_partition_on_delete_label() OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() FROM public ;
GRANT ALL ON FUNCTION wh_nagios.drop_partition_on_delete_label() TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.drop_partition_on_delete_label() IS 'Trigger that drop a dedicated partition when a label is deleted from the table wh_nagios.labels.' ;

CREATE TRIGGER create_partition_on_insert_service
    BEFORE INSERT ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_label() ;
CREATE TRIGGER drop_partition_on_delete_service
    AFTER DELETE ON wh_nagios.labels
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_label() ;
