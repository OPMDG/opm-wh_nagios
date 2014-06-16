-- complain if script is sourced in psql, rather than via CREATE EXTENSION
-- \echo Use "CREATE EXTENSION opm_core" to load this file. \quit

-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2014: Open PostgreSQL Monitoring Development Group

/************************************************************************
IMPORTANT :
This update script should not be launched alone but should be triggered
by updating opm-core. It should fail if launched first and opm-core
is still in version 1.1.
Earlier versions was not production ready, next updates won't have
that kind of issue.
***********************************************************************/

SET statement_timeout TO 0 ;

SET client_encoding = 'UTF8' ;
SET check_function_bodies = false ;

/*
As the tables/views that needs to be updated have dependancies,
the update is in two parts :
  p1) remove old dependancies, creates new tables
  p2) remove old tables, add dependancies
*/


/*
Handle view wh_nagios.services_labels (renamed to wh_nagios.services_metrics)
*/

DROP VIEW wh_nagios.services_labels;

/* Handle pr_grapher.graph_wn_nagios (renamed to wh_nagios.series) :
  - remove foreign key from pr_grapher.graph_wh_nagios to pr_grapher.graphs (p1)
  - remove foreign key from pr_grapher.graph_wh_nagios to wh_nagios.labels (p1)
  - create a new table (p1)
  - keep all existing data (p1)
  - inherits from public.series (p1)
  - tell pg_dump to dump its content (p1)
  - add foreign key from wh_nagios.series to public.graphs (p2)
  - add foreign key from wh_nagios.series to wh_nagios.metrics (p2)
  - old table will be deleted when removing pr_grapher_wh_nagios extension
*/

ALTER TABLE pr_grapher.graph_wh_nagios
  DROP CONSTRAINT graph_wh_nagios_id_graph_fkey ;
ALTER TABLE pr_grapher.graph_wh_nagios
  DROP CONSTRAINT graph_wh_nagios_id_label_fkey ;

CREATE TABLE wh_nagios.series (

)
INHERITS (public.series);
ALTER TABLE wh_nagios.series OWNER TO opm ;
REVOKE ALL ON wh_nagios.series FROM public ;
CREATE UNIQUE INDEX ON wh_nagios.series (id_metric, id_graph);
CREATE INDEX ON wh_nagios.series (id_graph);

COMMENT ON TABLE wh_nagios.series IS 'Stores all series for graph purpose.' ;
COMMENT ON COLUMN wh_nagios.series.id_graph IS 'Graph this serie is referencing.' ;
COMMENT ON COLUMN wh_nagios.series.id_metric IS 'Metric this serie is referencing.' ;
COMMENT ON COLUMN wh_nagios.series.config IS 'Specific config for this serie' ;

INSERT INTO wh_nagios.series (id_graph, id_metric) 
  SELECT id_graph, id_label
  FROM pr_grapher.graph_wh_nagios ;


ALTER EXTENSION pr_grapher_wh_nagios DROP TABLE pr_grapher.graph_wh_nagios;
DROP TABLE pr_grapher.graph_wh_nagios;

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.series', '') ;

-- Clean functions depending on tables
DROP FUNCTION wh_nagios.dispatch_record(int, bool);
DROP FUNCTION wh_nagios.cleanup_service(bigint);
DROP FUNCTION wh_nagios.purge_services(bigint[]);
DROP FUNCTION wh_nagios.delete_labels(bigint[]) ;
ALTER EXTENSION pr_grapher_wh_nagios DROP FUNCTION pr_grapher.create_graph_for_wh_nagios(bigint);
ALTER EXTENSION pr_grapher_wh_nagios DROP FUNCTION pr_grapher.list_wh_nagios_graphs();
ALTER EXTENSION pr_grapher_wh_nagios DROP FUNCTION pr_grapher.list_wh_nagios_labels(bigint);
ALTER EXTENSION pr_grapher_wh_nagios DROP FUNCTION pr_grapher.update_graph_labels(bigint, bigint[]);
ALTER EXTENSION pr_grapher DROP FUNCTION pr_grapher.list_graph();
ALTER EXTENSION pr_grapher DROP FUNCTION pr_grapher.delete_graph(bigint);

--DROP FUNCTION pr_grapher.list_wh_nagios_labels_config(bigint);
DROP FUNCTION pr_grapher.create_graph_for_wh_nagios(bigint);
DROP FUNCTION pr_grapher.list_wh_nagios_graphs();              
DROP FUNCTION pr_grapher.list_wh_nagios_labels(bigint);
DROP FUNCTION pr_grapher.update_graph_labels(bigint, bigint[]);
DROP FUNCTION pr_grapher.list_graph();
DROP FUNCTION pr_grapher.delete_graph(bigint);
DROP FUNCTION wh_nagios.list_label(bigint) ;

/*
Handle wh_nagios.labels table (renamed to wh_nagios.metrics) :
  - remove foreign key  from wh_nagios.labels to wh_nagios.services (p1)
  - remove foreign key from pr_grapher.graph_wh_nagios to wh_nagios.labels (p1)
  - remove function wh_nagios.create_partition_on_insert_label()
  - remove function wh_nagios.drop_partition_on_delete_label()
  - remove trigger create_partition_on_insert_label
  - remove trigger drop_partition_on_delete_label
  - create a new table (p1)
  - keep all existing data (p1)
  - inherits from public.metrics (p1)
  - tell pg_dump to dump its content (p1)
  - set current value for sequence
  - add foreign key from wh_nagios.metrics to wh_nagios.services (p2)
  - add foreign key from wh_nagios.series to wh_nagios.metrics (p2)
  - add function wh_nagios.create_partition_on_insert_metric()
  - add function wh_nagios.drop_partition_on_delete_metric()
  - add trigger create_partition_on_insert_metric (p2)
  - add trigger drop_partition_on_delete_metric (p2)
  - delete old table wh_nagios.labels
*/

ALTER TABLE wh_nagios.labels
  DROP CONSTRAINT wh_nagios_labels_fk ;
DROP TRIGGER create_partition_on_insert_label ON wh_nagios.labels;
DROP TRIGGER drop_partition_on_delete_label ON wh_nagios.labels;
DROP FUNCTION wh_nagios.create_partition_on_insert_label() ;
DROP FUNCTION wh_nagios.drop_partition_on_delete_label() ;

CREATE TABLE wh_nagios.metrics (
    min             numeric,
    max             numeric,
    critical        numeric,
    warning         numeric
)
INHERITS (public.metrics) ;
ALTER TABLE wh_nagios.metrics OWNER TO opm ;
REVOKE ALL ON wh_nagios.metrics FROM public ;
ALTER TABLE wh_nagios.metrics ADD PRIMARY KEY (id) ;
CREATE INDEX ON wh_nagios.metrics USING btree (id_service) ;
COMMENT ON TABLE wh_nagios.metrics IS 'Stores all metrics from services.' ;
COMMENT ON COLUMN wh_nagios.metrics.id IS 'Metric unique identifier. Is the primary key of table wh_nagios.metrics' ;
COMMENT ON COLUMN wh_nagios.metrics.id_service IS 'Referenced service in wh_nagios.' ;
COMMENT ON COLUMN wh_nagios.metrics.label IS 'Title of metric.' ;
COMMENT ON COLUMN wh_nagios.metrics.unit IS 'Unit of the metric.' ;
COMMENT ON COLUMN wh_nagios.metrics.min IS 'Min value for the metric.' ;
COMMENT ON COLUMN wh_nagios.metrics.max IS 'Max value for the metric.' ;
COMMENT ON COLUMN wh_nagios.metrics.critical IS 'Critical threshold for the metric.' ;
COMMENT ON COLUMN wh_nagios.metrics.warning IS 'Warning threshold for the metric.' ;

INSERT INTO wh_nagios.metrics (id, id_service, label, unit, min, max, critical, warning)
  SELECT id, id_service, label, unit, min, max, critical, warning
  FROM wh_nagios.labels;
SELECT setval('metrics_id_seq', (SELECT last_value FROM wh_nagios.labels_id_seq));

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.metrics', '') ;



/*
Part 2 of table handling :
  - wh_nagios.series
  - wh_nagios.metrics
  - wh_nagios.services_metrics
*/

-- wh_nagios.series
ALTER TABLE wh_nagios.series ADD CONSTRAINT fk_wh_nagios_series_series
    FOREIGN KEY (id_graph)
    REFERENCES public.graphs (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE ;
ALTER TABLE wh_nagios.series ADD CONSTRAINT fk_wh_nagios_series_wh_nagios_metrics
    FOREIGN KEY (id_metric)
    REFERENCES wh_nagios.metrics (id) MATCH FULL
    ON DELETE CASCADE ON UPDATE CASCADE ;

-- wh_nagios._metrics
ALTER EXTENSION wh_nagios DROP TABLE wh_nagios.labels;
alter table wh_nagios.labels drop constraint labels_pkey;
alter extension wh_nagios drop sequence wh_nagios.labels_id_seq ;
ALTER TABLE wh_nagios.labels DROP COLUMN id;
DROP TABLE wh_nagios.labels;
ALTER TABLE wh_nagios.metrics ADD CONSTRAINT fk_wh_nagios_metrics_wh_nagios_services
  FOREIGN KEY (id_service)
  REFERENCES wh_nagios.services (id) MATCH FULL
  ON DELETE CASCADE ON UPDATE CASCADE ;

-- wh_nagios.services_metrics
CREATE OR REPLACE VIEW wh_nagios.services_metrics AS
    SELECT s.id, s.id_server, s.warehouse, s.service, s.last_modified,
        s.creation_ts, s.last_cleanup, s.servalid, s.state, m.min,
        m.max, m.critical, m.warning, s.oldest_record, s.newest_record,
        m.id as id_metric, m.label, m.unit
    FROM wh_nagios.services s
    JOIN wh_nagios.metrics m
        ON s.id = m.id_service ;

ALTER VIEW wh_nagios.services_metrics OWNER TO opm ;
REVOKE ALL ON wh_nagios.services_metrics FROM public ;
COMMENT ON VIEW wh_nagios.services_metrics IS 'All informations for all services, and metrics
if the service has metrics.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.id IS 'Service unique identifier' ;
COMMENT ON COLUMN wh_nagios.services_metrics.id_server IS 'Identifier of the server' ;
COMMENT ON COLUMN wh_nagios.services_metrics.warehouse IS 'warehouse that stores this specific metric' ;
COMMENT ON COLUMN wh_nagios.services_metrics.service IS 'service name that provides a specific metric' ;
COMMENT ON COLUMN wh_nagios.services_metrics.last_modified IS 'last day that the dispatcher pushed datas in the warehouse' ;
COMMENT ON COLUMN wh_nagios.services_metrics.creation_ts IS 'warehouse creation date and time for this particular service' ;
COMMENT ON COLUMN wh_nagios.services_metrics.last_cleanup IS 'Last launch of "specific-warehouse".cleanup_service(). Each warehouse has to implement his own, if needed.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.servalid IS 'data retention time' ;
COMMENT ON COLUMN wh_nagios.services_metrics.state IS 'Current nagios state of the service
(OK,WARNING,CRITICAL or UNKNOWN). This state is not timestamped.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.min IS 'Min value for the metric.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.max IS 'Max value for the metric.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.critical IS 'Critical threshold for the metric.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.warning IS 'Warning threshold for the metric.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.oldest_record IS 'Timestamp of the oldest value stored for the service.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.newest_record IS 'Timestamp of the newest value stored for the service.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.id_metric IS 'Metric unique identifier. Is the primary key of table wh_nagios.metrics' ;
COMMENT ON COLUMN wh_nagios.services_metrics.label IS 'Title of metric.' ;
COMMENT ON COLUMN wh_nagios.services_metrics.unit IS 'Unit of the metric.' ;

/* wh_nagios.cleanup_service(bigint)
Aggregate all data by day in an array, to avoid space overhead and benefit TOAST compression.
This will be done for every metric corresponding to the service.

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
    FOR v_partid IN SELECT id FROM wh_nagios.metrics WHERE id_service = p_serviceid LOOP
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
This will be done for every metric corresponding to the service.' ;

--Automatically create a new partition when a service is added.
CREATE OR REPLACE FUNCTION wh_nagios.create_partition_on_insert_metric() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_rolname name ;
BEGIN
    EXECUTE format('CREATE TABLE wh_nagios.counters_detail_%s (date_records date, records public.metric_value[])', NEW.id) ;
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

ALTER FUNCTION wh_nagios.create_partition_on_insert_metric() OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.create_partition_on_insert_metric() FROM public ;
GRANT ALL ON FUNCTION wh_nagios.create_partition_on_insert_metric() TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.create_partition_on_insert_metric() IS 'Trigger that create a dedicated partition when a new metric is inserted in the table wh_nagios.metrics,
and GRANT the necessary ACL on it.
If the dedicated partition is alreay present (which should not happen, due to the other trigger), it''s truncated.' ;

/* wh_nagios.delete_metrics(VARIADIC bigint[])
Delete specific metrics.

Tigger will drop related partitions.

@p_metricsid: Unique identifiers of the metrics to delete.
@return : true if eveything went well.
*/
CREATE OR REPLACE function wh_nagios.delete_metrics(VARIADIC p_metricsid bigint[])
    RETURNS boolean
    AS $$
DECLARE
  v_state      text ;
  v_msg        text ;
  v_detail     text ;
  v_hint       text ;
  v_context    text ;
  v_metricsid text ;
BEGIN
    v_metricsid := array_to_string(p_metricsid, ',');
    EXECUTE format('DELETE FROM wh_nagios.metrics WHERE id IN ( %s ) ', v_metricsid ) ;
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

ALTER FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[])
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[])
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[])
    TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.delete_metrics(VARIADIC bigint[]) IS 'Delete metrics.
The corresponding partitions will be dropped.' ;

-- Change comment on function wh_nagios.delete_services
COMMENT ON FUNCTION wh_nagios.delete_services(VARIADIC bigint[]) IS 'Delete a service.
All related metrics will also be deleted, and the corresponding partitions
will be dropped.' ;

/* wh_nagios.dispatch_record(boolean, integer)
Dispatch records from wh_nagios.hub into counters_detail_$ID

$ID is found in wh_nagios.services_metric and wh_nagios.services, with correct hostname,servicedesc and label

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
    metricsrow wh_nagios.metrics%ROWTYPE ;
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
            metricsrow := NULL ;

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

                --Does the metric exists ?
                SELECT l.* INTO metricsrow
                FROM wh_nagios.metrics AS l
                WHERE id_service = servicesrow.id
                    AND label = (cur->'label') ;

                IF NOT FOUND THEN
                    msg_err := 'Error during INSERT on wh_nagios.metrics: %L - %L' ;

                    -- The trigger on wh_nagios.services_metric will create the partition counters_detail_$service_id automatically
                    INSERT INTO wh_nagios.metrics (id_service, label, unit, min, max, warning, critical)
                    VALUES (servicesrow.id, cur->'label', cur->'uom', (cur->'min')::numeric, (cur->'max')::numeric, (cur->'warning')::numeric, (cur->'critical')::numeric)
                    RETURNING * INTO STRICT metricsrow ;
                END IF ;

                --Do we need to update the metric ?
                IF ( ( (cur->'uom') IS NOT NULL AND (metricsrow.unit <> (cur->'uom') OR (metricsrow.unit IS NULL)) )
                    OR ( (cur->'min') IS NOT NULL AND (metricsrow.min <> (cur->'min')::numeric OR (metricsrow.min IS NULL)) )
                    OR ( (cur->'max') IS NOT NULL AND (metricsrow.max <> (cur->'max')::numeric OR (metricsrow.max IS NULL)) )
                    OR ( (cur->'warning') IS NOT NULL AND (metricsrow.warning <> (cur->'warning')::numeric OR (metricsrow.warning IS NULL)) )
                    OR ( (cur->'critical') IS NOT NULL AND (metricsrow.critical <> (cur->'critical')::numeric OR (metricsrow.critical IS NULL)) )
                ) THEN
                    msg_err := 'Error during UPDATE on wh_nagios.metrics: %L - %L' ;

                    EXECUTE format('UPDATE wh_nagios.metrics SET
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
                    ) USING metricsrow.id ;
                END IF ;


                IF (servicesrow.id IS NOT NULL AND servicesrow.last_cleanup < now() - '10 days'::interval) THEN
                    PERFORM wh_nagios.cleanup_service(servicesrow.id) ;
                END IF ;


                msg_err := format('Error during INSERT on counters_detail_%s: %%L - %%L', metricsrow.id) ;

                EXECUTE format(
                    'INSERT INTO wh_nagios.counters_detail_%s (date_records,records)
                    VALUES (
                        date_trunc(''day'',timestamp with time zone ''epoch''+%L * INTERVAL ''1 second''),
                        array[row(timestamp with time zone ''epoch''+%L * INTERVAL ''1 second'',%L )]::public.metric_value[]
                    )',
                    metricsrow.id,
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

--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_metric()
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

ALTER FUNCTION wh_nagios.drop_partition_on_delete_metric() OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.drop_partition_on_delete_metric() FROM public ;
GRANT ALL ON FUNCTION wh_nagios.drop_partition_on_delete_metric() TO opm_admins ;
COMMENT ON FUNCTION wh_nagios.drop_partition_on_delete_metric() IS 'Trigger that drop a dedicated partition when a metric is deleted from the table wh_nagios.metrics.' ;

DROP FUNCTION wh_nagios.get_sampled_label_data(bigint,timestamp with time zone,timestamp with time zone,integer) ;
DROP FUNCTION wh_nagios.get_sampled_label_data(text,text,text,timestamp with time zone,timestamp with time zone,integer) ;

CREATE FUNCTION wh_nagios.get_metric_data(id_metric bigint, timet_begin timestamp with time zone, timet_end timestamp with time zone)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('SELECT (unnest(records)).* FROM wh_nagios.counters_detail_%s WHERE date_records >= $1 AND date_records <= $2', id_metric) USING timet_begin,timet_end ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone)
    TO opm_roles ;
COMMENT ON FUNCTION wh_nagios.get_metric_data(bigint, timestamp with time zone, timestamp with time zone) IS 'Return metric data for the specified metric unique identifier within the specified interval.' ;


CREATE FUNCTION wh_nagios.get_metric_data(i_hostname text, i_service text, i_label text, timet_begin timestamp with time zone, timet_end timestamp with time zone)
RETURNS TABLE(timet timestamp with time zone, value numeric)
AS $$
DECLARE
    v_id_metric bigint ;
BEGIN
    SELECT id INTO v_id_metric
    FROM wh_nagios.services_metric
    WHERE hostname = i_hostname
        AND service = i_service
        AND label = i_label ;

    IF NOT FOUND THEN
        RETURN ;
    ELSE
        RETURN QUERY SELECT * FROM wh_nagios.get_sampled_metric_data(v_id_metric,timet_begin,timet_end) ;
    END IF ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF ;

ALTER FUNCTION wh_nagios.get_metric_data(text, text, text, timestamp with time zone, timestamp with time zone)
    OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.get_metric_data(text, text, text, timestamp with time zone, timestamp with time zone)
    FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.get_metric_data(text, text, text, timestamp with time zone, timestamp with time zone)
    TO opm_roles ;
COMMENT ON FUNCTION wh_nagios.get_metric_data(text, text, text, timestamp with time zone, timestamp with time zone) IS 'Return metric data for the specified hostname, service and metric (all by name) within the specified interval.' ;

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
        v_metric_id    bigint ;
BEGIN
    FOR v_metric_id IN (SELECT id_metric FROM wh_nagios.list_metrics(p_service_id))
    LOOP
        EXECUTE format('GRANT SELECT ON wh_nagios.counters_detail_%s TO %I', v_metric_id, p_rolname) ;
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

/* wh_nagios.list_metrics(bigint)
Return every id and metric for a service

@service_id: service wanted
@return : id, labeln unit, min, max, critical and warning for metrics
*/
CREATE OR REPLACE FUNCTION wh_nagios.list_metrics(p_service_id bigint)
RETURNS TABLE (id_metric bigint, label text, unit text, min numeric,
    max numeric, critical numeric, warning numeric)
AS $$
DECLARE
BEGIN
    IF public.is_admin(session_user) THEN
        RETURN QUERY SELECT m.id, m.label, m.unit, m.min, m.max, m.critical, m.warning
            FROM wh_nagios.metrics m
            JOIN wh_nagios.services s
                ON s.id = m.id_service
            WHERE s.id = p_service_id ;
    ELSE
        RETURN QUERY SELECT m.id, m.label, m.unit, m.min, m.max, m.critical, m.warning
            FROM wh_nagios.list_services() s
            JOIN wh_nagios.metrics m
                ON s.id = m.id_service
            WHERE s.id = p_service_id
        ;
        END IF ;
END ;
$$
LANGUAGE plpgsql
STABLE
LEAKPROOF
SECURITY DEFINER ;

ALTER FUNCTION wh_nagios.list_metrics(bigint) OWNER TO opm ;
REVOKE ALL ON FUNCTION wh_nagios.list_metrics(bigint) FROM public ;
GRANT EXECUTE ON FUNCTION wh_nagios.list_metrics(bigint) TO opm_roles ;

COMMENT ON FUNCTION wh_nagios.list_metrics(bigint) IS 'Return all metrics for a specific service, if user is allowed to.' ;

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

            FOR v_partid IN SELECT id FROM wh_nagios.metrics WHERE id_service = v_serviceid LOOP
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
        v_metric_id    bigint ;
BEGIN
    FOR v_metric_id IN (SELECT id_metric FROM wh_nagios.list_metrics(p_service_id))
    LOOP
        EXECUTE format('REVOKE SELECT ON wh_nagios.counters_detail_%s FROM %I', v_metric_id, p_rolname) ;
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

CREATE TRIGGER create_partition_on_insert_metric
    BEFORE INSERT ON wh_nagios.metrics
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_metric();

CREATE TRIGGER drop_partition_on_delete_metric
    AFTER DELETE ON wh_nagios.metrics
    FOR EACH ROW
    EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_metric();
