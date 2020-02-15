-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2020: Open PostgreSQL Monitoring Development Group

/***************************************
*
* Make sure query won't get cancelled
* and handle default ACL
*
***************************************/

SET statement_timeout TO 0;

REVOKE ALL ON SCHEMA wh_nagios FROM public;

/***************************************
*
* Create extension's objects
*
***************************************/

CREATE TABLE wh_nagios.hub (
    id bigserial,
    rolname text NOT NULL default current_user,
    data text[]
);

REVOKE ALL ON TABLE wh_nagios.hub FROM public;

COMMENT ON TABLE  wh_nagios.hub         IS 'Store raw perfdata from dispatchers. Those
data will be processed asynchronously by stored function wh_nagios.dispatch_record().
This table doesn''t have a primary key.';
COMMENT ON COLUMN wh_nagios.hub.id      IS 'Batch identifier of the data importation.';
COMMENT ON COLUMN wh_nagios.hub.rolname IS 'User who inserted data.';
COMMENT ON COLUMN wh_nagios.hub.data    IS 'Raw data as sent by dispatchers.';


CREATE TABLE wh_nagios.hub_reject (
    id bigserial NOT NULL,
    rolname text NOT NULL,
    data text[],
    msg text
);

REVOKE ALL ON TABLE wh_nagios.hub_reject FROM public;

COMMENT ON TABLE  wh_nagios.hub_reject         IS 'Store hub lines rejected by the
stored function wh_nagios.dispatch_record(), if it''s asked to log them.
This table doesn''t have a primary key.';
COMMENT ON COLUMN wh_nagios.hub_reject.id      IS 'Batch identifier of failed data importation.';
COMMENT ON COLUMN wh_nagios.hub_reject.rolname IS 'User who inserted failed data.';
COMMENT ON COLUMN wh_nagios.hub_reject.data    IS 'Raw data as sent by dispatchers.';
COMMENT ON COLUMN wh_nagios.hub_reject.msg     IS 'Error message sent from wh_nagios.dispatch_record().';

CREATE TABLE wh_nagios.services (
    state text,
    PRIMARY KEY (id),
    FOREIGN KEY (id_server) REFERENCES public.servers (id) ON UPDATE CASCADE ON DELETE CASCADE,
    UNIQUE (id_server, service)
)
INHERITS (public.services);

REVOKE ALL ON TABLE wh_nagios.services FROM public;

COMMENT ON TABLE  wh_nagios.services               IS 'Lists all available metrics for warehouse "wh_nagios". It''s inherited from the table public.services.';
COMMENT ON COLUMN wh_nagios.services.id            IS 'Service unique identifier. Is the primary key.';
COMMENT ON COLUMN wh_nagios.services.id_server     IS 'Identifier of the server.';
COMMENT ON COLUMN wh_nagios.services.warehouse     IS 'warehouse that stores this specific metric. Fixed value of "wh_nagios" for this partition.';
COMMENT ON COLUMN wh_nagios.services.service       IS 'service name that provides a specific metric.';
COMMENT ON COLUMN wh_nagios.services.last_modified IS 'last day that the dispatcher pushed datas in the warehouse.';
COMMENT ON COLUMN wh_nagios.services.creation_ts   IS 'warehouse creation date and time for this particular service.';
COMMENT ON COLUMN wh_nagios.services.last_cleanup  IS 'Last launch of "warehouse".cleanup_service(). Each warehouse has to implement his own, if needed.';
COMMENT ON COLUMN wh_nagios.services.servalid      IS 'data retention time.';
COMMENT ON COLUMN wh_nagios.services.oldest_record IS 'Timestamp of the oldest value stored for the service.';
COMMENT ON COLUMN wh_nagios.services.newest_record IS 'Timestamp of the newest value stored for the service.';
COMMENT ON COLUMN wh_nagios.services.state         IS 'Current nagios state of the service
(OK,WARNING,CRITICAL or UNKNOWN). This state is not timestamped.';

CREATE TABLE wh_nagios.metrics (
    min         numeric,
    max         numeric,
    critical    numeric,
    warning     numeric,
    PRIMARY KEY (id),
    FOREIGN KEY (id_service) REFERENCES wh_nagios.services (id) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE
)
INHERITS (public.metrics);

REVOKE ALL ON wh_nagios.metrics FROM public;

CREATE INDEX ON wh_nagios.metrics USING btree (id_service);

COMMENT ON TABLE  wh_nagios.metrics            IS 'Stores all metrics from services.';
COMMENT ON COLUMN wh_nagios.metrics.id         IS 'Metric unique identifier. Is the primary key of table wh_nagios.metrics';
COMMENT ON COLUMN wh_nagios.metrics.id_service IS 'Referenced service in wh_nagios.';
COMMENT ON COLUMN wh_nagios.metrics.label      IS 'Title of metric.';
COMMENT ON COLUMN wh_nagios.metrics.unit       IS 'Unit of the metric.';
COMMENT ON COLUMN wh_nagios.metrics.min        IS 'Min value for the metric.';
COMMENT ON COLUMN wh_nagios.metrics.max        IS 'Max value for the metric.';
COMMENT ON COLUMN wh_nagios.metrics.critical   IS 'Critical threshold for the metric.';
COMMENT ON COLUMN wh_nagios.metrics.warning    IS 'Warning threshold for the metric.';

CREATE TABLE wh_nagios.series (
    FOREIGN KEY (id_graph)  REFERENCES public.graphs (id)     MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (id_metric) REFERENCES wh_nagios.metrics (id) MATCH FULL ON DELETE CASCADE ON UPDATE CASCADE
)
INHERITS (public.series);
CREATE UNIQUE INDEX ON wh_nagios.series (id_metric, id_graph);
CREATE INDEX ON wh_nagios.series (id_graph);

REVOKE ALL ON wh_nagios.series FROM public ;

COMMENT ON TABLE  wh_nagios.series           IS 'Stores all series for graph purpose.';
COMMENT ON COLUMN wh_nagios.series.id_graph  IS 'Graph this serie is referencing.';
COMMENT ON COLUMN wh_nagios.series.id_metric IS 'Metric this serie is referencing.';
COMMENT ON COLUMN wh_nagios.series.config    IS 'Specific config for this serie';

CREATE OR REPLACE VIEW wh_nagios.services_metrics AS
    SELECT s.id, s.id_server, s.warehouse, s.service, s.last_modified,
        s.creation_ts, s.last_cleanup, s.servalid, s.state, m.min,
        m.max, m.critical, m.warning, s.oldest_record, s.newest_record,
        m.id as id_metric, m.label, m.unit
    FROM wh_nagios.services s
    JOIN wh_nagios.metrics m
        ON s.id = m.id_service;

REVOKE ALL ON wh_nagios.services_metrics FROM public;

COMMENT ON VIEW   wh_nagios.services_metrics               IS 'All informations for all services, and metrics
if the service has metrics.';
COMMENT ON COLUMN wh_nagios.services_metrics.id            IS 'Service unique identifier';
COMMENT ON COLUMN wh_nagios.services_metrics.id_server     IS 'Identifier of the server';
COMMENT ON COLUMN wh_nagios.services_metrics.warehouse     IS 'warehouse that stores this specific metric';
COMMENT ON COLUMN wh_nagios.services_metrics.service       IS 'service name that provides a specific metric';
COMMENT ON COLUMN wh_nagios.services_metrics.last_modified IS 'last day that the dispatcher pushed datas in the warehouse';
COMMENT ON COLUMN wh_nagios.services_metrics.creation_ts   IS 'warehouse creation date and time for this particular service';
COMMENT ON COLUMN wh_nagios.services_metrics.last_cleanup  IS 'Last launch of "specific-warehouse".cleanup_service(). Each warehouse has to implement his own, if needed.';
COMMENT ON COLUMN wh_nagios.services_metrics.servalid      IS 'data retention time';
COMMENT ON COLUMN wh_nagios.services_metrics.min           IS 'Min value for the metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.max           IS 'Max value for the metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.critical      IS 'Critical threshold for the metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.warning       IS 'Warning threshold for the metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.oldest_record IS 'Timestamp of the oldest value stored for the service.';
COMMENT ON COLUMN wh_nagios.services_metrics.newest_record IS 'Timestamp of the newest value stored for the service.';
COMMENT ON COLUMN wh_nagios.services_metrics.id_metric     IS 'Metric unique identifier. Is the primary key of table wh_nagios.metrics';
COMMENT ON COLUMN wh_nagios.services_metrics.label         IS 'Title of metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.unit          IS 'Unit of the metric.';
COMMENT ON COLUMN wh_nagios.services_metrics.state         IS 'Current nagios state of the service
(OK,WARNING,CRITICAL or UNKNOWN). This state is not timestamped.';

/***************************************
*
* Tell pg_dump which objects to dump
*
***************************************/

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.services', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.metrics', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.series', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_id_seq', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_reject', '') ;
SELECT pg_catalog.pg_extension_config_dump('wh_nagios.hub_reject_id_seq', '') ;


/***************************************
*
* Create extension's functions
*
***************************************/



/*********** NOT API *************/



/* v2.1
wh_nagios.grant_dispatcher(role)

@return rc: state of the operation
 */
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

/* v2.1
wh_nagios.revoke_dispatcher(role)

@return rc: state of the operation
 */
CREATE OR REPLACE
FUNCTION wh_nagios.revoke_dispatcher( IN p_rolname name )
RETURNS TABLE (operat text, approle name, appright text, objtype text, objname text)
LANGUAGE plpgsql STRICT VOLATILE LEAKPROOF SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
    v_dbname name := pg_catalog.current_database();
BEGIN
    operat   := 'REVOKE';
    approle  := p_rolname;

    appright := 'ALL';
    objtype  := 'DATABASE';
    objname  := v_dbname;
    EXECUTE pg_catalog.format('REVOKE %s ON %s %I FROM %I', appright, objtype, objname, approle);
    RETURN NEXT;

    objtype  := 'SCHEMA';
    objname  := 'wh_nagios';
    EXECUTE pg_catalog.format('REVOKE %s ON %s %I FROM %I', appright, objtype, objname, approle);
    RETURN NEXT;

    objtype  := 'SEQUENCE';
    objname  := 'wh_nagios.hub_id_seq';
    EXECUTE pg_catalog.format('REVOKE %s ON %s %s FROM %I', appright, objtype, objname, approle);
    RETURN NEXT;

    objtype  := 'TABLE';
    objname  := 'wh_nagios.hub';
    EXECUTE pg_catalog.format('REVOKE %s ON %s %s FROM %I', appright, objtype, objname, approle);
    RETURN NEXT;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) FROM public;

COMMENT ON FUNCTION wh_nagios.revoke_dispatcher(IN name, OUT boolean) IS
'Revoke dispatch performance data from a role in wh_nagios.';


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

REVOKE ALL ON FUNCTION wh_nagios.create_partition_on_insert_metric() FROM public;

COMMENT ON FUNCTION wh_nagios.create_partition_on_insert_metric() IS
'Trigger that create a dedicated partition when a new metric is inserted in the table wh_nagios.metrics,
and GRANT the necessary ACL on it.';


--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_metric()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DROP TABLE wh_nagios.counters_detail_%s', OLD.id) ;
    RETURN NULL;
EXCEPTION
    WHEN undefined_table THEN
        RETURN NULL;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.drop_partition_on_delete_metric() FROM public;

COMMENT ON FUNCTION wh_nagios.drop_partition_on_delete_metric() IS
'Trigger that drop a dedicated partition when a metric is deleted from the table wh_nagios.metrics.';

CREATE TRIGGER create_partition_on_insert_metric
    BEFORE INSERT ON wh_nagios.metrics
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_metric();

CREATE TRIGGER drop_partition_on_delete_metric
    AFTER DELETE ON wh_nagios.metrics
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_metric();



/*********** API *************/

/* wh_nagios.list_metrics(bigint)
Return every id and metric for a service

@service_id: service wanted
@return : id, labeln unit, min, max, critical and warning for metrics
*/
CREATE OR REPLACE
FUNCTION wh_nagios.list_metrics(p_service_id bigint)
RETURNS TABLE (id_metric bigint, label text, unit text, min numeric,
               max numeric, critical numeric, warning numeric)
LANGUAGE plpgsql LEAKPROOF STRICT STABLE SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
BEGIN
    IF public.is_admin() THEN
        RETURN QUERY SELECT m.id, m.label, m.unit, m.min, m.max, m.critical, m.warning
            FROM wh_nagios.metrics m
            JOIN wh_nagios.services s
                ON s.id = m.id_service
            WHERE s.id = p_service_id;
    ELSE
        RETURN QUERY SELECT m.id, m.label, m.unit, m.min, m.max, m.critical, m.warning
            FROM wh_nagios.list_services() AS s
            JOIN wh_nagios.metrics m
                ON s.id = m.id_service
            WHERE s.id = p_service_id;
    END IF;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.list_metrics(bigint) FROM public ;

COMMENT ON FUNCTION wh_nagios.list_metrics(bigint) IS
'Return all metrics for given service by id, if user is allowed to.' ;

SELECT * FROM public.register_api('wh_nagios.list_metrics(bigint)'::regprocedure);


/* wh_nagios.list_services()
Return every wh_nagios.services%ROWTYPE

@return : wh_nagios.services%ROWTYPE
*/
CREATE OR REPLACE
FUNCTION wh_nagios.list_services()
RETURNS SETOF wh_nagios.services
LANGUAGE plpgsql STRICT STABLE SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    IF public.is_admin() THEN
        RETURN QUERY SELECT s.*
            FROM wh_nagios.services AS s;
    ELSE
        RETURN QUERY SELECT ser.*
            FROM public.list_servers() AS srv
            JOIN wh_nagios.services AS ser
                ON srv.id = ser.id_server ;
    END IF;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.list_services() FROM public;

COMMENT ON FUNCTION wh_nagios.list_services() IS
'Return all services a user is allowed to see.';

SELECT * FROM public.register_api('wh_nagios.list_services()'::regprocedure);


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

REVOKE ALL ON FUNCTION wh_nagios.cleanup_service(bigint) FROM public;

COMMENT ON FUNCTION wh_nagios.cleanup_service(bigint) IS
'Aggregate all data by day in an array, to avoid space overhead and benefit TOAST compression.
This will be done for every metric corresponding to the service.';

SELECT * FROM public.register_api('wh_nagios.cleanup_service(bigint)'::regprocedure);


/* wh_nagios.purge_services(VARIADIC bigint[])
Delete records older than max(date_records) - service.servalid. Doesn't delete
any data if servalid IS NULL

@p_serviceid: ID's of service to purge. All services if null.
@return : number of services purged.
*/
CREATE OR REPLACE
FUNCTION wh_nagios.purge_services(VARIADIC p_servicesid bigint[] = NULL)
RETURNS bigint
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
DECLARE
  i              bigint;
  v_allservices  bigint[];
  v_serviceid    bigint;
  v_servicefound boolean;
  v_partid       bigint;
  v_partname     text;
  v_servalid     interval;
  v_ret          bigint;
  v_oldest       timestamptz;
  v_oldtmp       timestamptz;
BEGIN
    v_ret := 0;

    IF p_servicesid IS NULL THEN
        SELECT pg_catalog.array_agg(id) INTO v_allservices
        FROM wh_nagios.services
        WHERE servalid IS NOT NULL;
    ELSE
        v_allservices := p_servicesid;
    END IF;

    IF v_allservices IS NULL THEN
        return v_ret;
    END IF;

    FOR i IN 1..pg_catalog.array_upper(v_allservices, 1) LOOP
        v_serviceid := v_allservices[i];
        SELECT pg_catalog.COUNT(1) = 1 INTO v_servicefound
        FROM wh_nagios.services
        WHERE id = v_serviceid
            AND servalid IS NOT NULL;

        IF v_servicefound THEN
            v_ret := v_ret + 1;

            SELECT servalid INTO STRICT v_servalid
            FROM wh_nagios.services
            WHERE id = v_serviceid;

            FOR v_partid IN SELECT id FROM wh_nagios.metrics WHERE id_service = v_serviceid LOOP
                v_partname := pg_catalog.format('counters_detail_%s', v_partid);

                EXECUTE pg_catalog.format('WITH m as ( SELECT pg_catalog.max(date_records) as max
                            FROM wh_nagios.%I
                        )
                    DELETE
                    FROM wh_nagios.%I c
                    USING m
                    WHERE age(m.max, c.date_records) >= %L::interval;
                ', v_partname, v_partname, v_servalid);

                EXECUTE pg_catalog.format('SELECT pg_catalog.min(timet)
                    FROM (
                      SELECT (pg_catalog.unnest(records)).timet
                      FROM (
                        SELECT records
                        FROM wh_nagios.%I
                        ORDER BY date_records ASC
                        LIMIT 1
                      )s
                    )s2', v_partname) INTO v_oldtmp;

                v_oldest := least(v_oldest, v_oldtmp);
            END LOOP;

            IF v_oldest IS NOT NULL THEN
                EXECUTE pg_catalog.format('UPDATE wh_nagios.services
                  SET oldest_record = %L
                  WHERE id = %s', v_oldest, v_serviceid);
            END IF;
        END IF;
    END LOOP;

    RETURN v_ret;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.purge_services(VARIADIC bigint[]) FROM public;

COMMENT ON FUNCTION wh_nagios.purge_services(VARIADIC bigint[]) IS
'Delete data older than retention interval.
The age is calculated from newest_record, not server date.';

SELECT * FROM public.register_api('wh_nagios.purge_services(bigint[])'::regprocedure);


/* wh_nagios.delete_services(VARIADIC bigint[])
Delete a specific service.

Foreign key will delete related metrics, and trigger will drop related partitions.

@p_serviceid: Unique identifiers of the services to deletes.
@return : the set of services id deleted if eveything went well.
*/
CREATE OR REPLACE
FUNCTION wh_nagios.delete_services(VARIADIC p_servicesid bigint[])
RETURNS TABLE (id_service bigint)
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    IF NOT public.is_admin() THEN
        RAISE EXCEPTION 'You must be an admin!';
    END IF;

    RETURN QUERY DELETE FROM wh_nagios.services
        WHERE id = ANY ( p_servicesid )
        RETURNING id AS id_service;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[]) FROM public;

COMMENT ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[]) IS
'Delete a service.

All related metrics will also be deleted, and the corresponding partitions
will be dropped.

User must be admin.';

SELECT * FROM public.register_api('wh_nagios.delete_services(bigint[])'::regprocedure);



/* wh_nagios.update_services_validity(interval, VARIADIC bigint[])
Update data retention of a specific service.

This function will not call pruge_services(), so data will stay until a purge
is manually executed, or next purge cron job if it exists.

@p_validity: New interval.
@p_servicesid: Unique identifiers of the services to update.
@return : the set of service id updated.
*/
CREATE OR REPLACE
FUNCTION wh_nagios.update_services_validity(p_validity interval, VARIADIC p_servicesid bigint[])
RETURNS TABLE (id_service bigint)
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    IF NOT public.is_admin() THEN
        RAISE EXCEPTION 'You must be an admin!';
    END IF;

    RETURN QUERY UPDATE wh_nagios.services
        SET servalid = p_validity
        WHERE id = ANY ( p_servicesid )
        RETURNING id AS id_service;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.update_services_validity(interval, bigint[]) FROM public;

COMMENT ON FUNCTION wh_nagios.update_services_validity(interval, bigint[]) IS
'Update validity of some services.

This function won''t automatically purge the related data.

User must be admin.';

SELECT * FROM public.register_api('wh_nagios.update_services_validity(interval, bigint[])'::regprocedure);


/* wh_nagios.delete_metrics(VARIADIC bigint[])
Delete specific metrics.

Tiggers will drop related partitions.

@p_metricsid: Unique identifiers of the metrics to delete.
@return : the metrics id deleted
*/
CREATE OR REPLACE
FUNCTION wh_nagios.delete_metrics(VARIADIC p_metricsid bigint[])
RETURNS TABLE (id_metric bigint)
LANGUAGE plpgsql STRICT VOLATILE SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    IF NOT public.is_admin() THEN
        RAISE EXCEPTION 'You must be an admin!';
    END IF;

    RETURN QUERY DELETE FROM wh_nagios.metrics
        WHERE id = ANY ( p_metricsid )
        RETURNING id AS id_metric;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[]) FROM public;

COMMENT ON FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[]) IS
'Delete given metrics.

The corresponding partitions will be dropped.

User must be admin.';

SELECT * FROM public.register_api('wh_nagios.delete_metrics(bigint[])'::regprocedure);


CREATE OR REPLACE
FUNCTION wh_nagios.get_metric_timespan(IN id_metric bigint)
RETURNS TABLE(min_date date, max_date date)
LANGUAGE plpgsql STABLE STRICT LEAKPROOF SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    -- FIXME check user rights to access these data ?
    RETURN QUERY EXECUTE format('
        SELECT min(date_records), max(date_records)
        FROM wh_nagios.counters_detail_%s', id_metric
    );
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.get_metric_timespan(bigint) FROM public;

COMMENT ON FUNCTION wh_nagios.get_metric_timespan(bigint) IS
'returns min and max known date for given metric id';

SELECT * FROM public.register_api('wh_nagios.get_metric_timespan(bigint)'::regprocedure);


CREATE OR REPLACE
FUNCTION wh_nagios.get_metric_data(id_metric bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone)
RETURNS TABLE(timet timestamp with time zone, value numeric)
LANGUAGE plpgsql STABLE STRICT SECURITY DEFINER
SET search_path TO public
AS $$
BEGIN
    -- FIXME check user rights to access these data ?
    RETURN QUERY EXECUTE format('
        SELECT * FROM (
            SELECT (pg_catalog.unnest(records)).*
            FROM wh_nagios.counters_detail_%s
            WHERE date_records >= date_trunc(''day'',$1)
                AND date_records <= date_trunc(''day'',$2)
        ) sql
        WHERE timet >= $1 AND timet <= $2', id_metric
    ) USING timet_begin,timet_end;
END
$$;

REVOKE ALL ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone)
    FROM public;

COMMENT ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone) IS
'Return metric data for the specified metric unique identifier within the specified interval.';

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


-- This line must be the last one, so that every functions are owned
-- by the database owner
SELECT * FROM public.set_extension_owner('wh_nagios');
