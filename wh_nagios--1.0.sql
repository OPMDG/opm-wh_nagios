-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

CREATE TABLE wh_nagios.hub (
	id bigserial,
	data text[]
);

ALTER TABLE wh_nagios.hub OWNER TO pgfactory;
REVOKE ALL ON TABLE wh_nagios.hub FROM public ;

CREATE TABLE wh_nagios.services (
	label		text,
	unit		text,
	state		text,
	min			numeric,
	max			numeric,
	critical	numeric,
	warning		numeric
)
INHERITS (public.services);

ALTER TABLE wh_nagios.services OWNER TO pgfactory;
ALTER TABLE wh_nagios.services ADD PRIMARY KEY (id);
CREATE UNIQUE INDEX idx_wh_nagios_services_hostname_service_label
    ON wh_nagios.services USING btree (hostname, service, label);
REVOKE ALL ON TABLE wh_nagios.services FROM public ;

SELECT pg_catalog.pg_extension_config_dump('wh_nagios.services', '');

CREATE OR REPLACE FUNCTION wh_nagios.dispatch_record() RETURNS boolean
	LANGUAGE plpgsql
	AS $$
DECLARE
	--Select current lines and lock them so then can be deleted
	--Use NOWAIT so there can't be two concurrent processes
	c_hub CURSOR FOR SELECT * FROM wh_nagios.hub FOR UPDATE NOWAIT;
	r_hub record;
	service_id integer;
    i integer;
    cur hstore;
BEGIN
/*
TODO: Handle seracl 

*/
	BEGIN
		FOR r_hub IN c_hub LOOP
            --Check 1 dimension,even number of data and at least 10 vals
			IF ((array_upper(r_hub.data,2) IS NULL) AND (array_upper(r_hub.data,1) > 9) AND ((array_upper(r_hub.data,1) % 2) = 0)) THEN
                cur := NULL;
                --Get all data as hstore
                FOR i IN 1..array_upper(r_hub.data,1) BY 2 LOOP
                    IF (cur IS NULL) THEN
                        cur := hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                    ELSE
                        cur := cur || hstore(lower(r_hub.data[i]),r_hub.data[i+1]);
                    END IF;
                END LOOP;

				--Do we have all informations needed ?
                IF ( ((cur->'hostname') IS NOT NULL) AND ((cur->'servicedesc') IS NOT NULL) AND ((cur->'label') IS NOT NULL) AND ((cur->'timet') IS NOT NULL) AND ((cur->'value') IS NOT NULL) ) THEN
                    --Does the service exists ?
    				SELECT id INTO service_id
    				FROM wh_nagios.services
    				WHERE hostname = (cur->'hostname')
    					AND service = (cur->'servicedesc')
    					AND label = (cur->'label');
				
    				IF NOT FOUND THEN
    					-- The trigger on wh_nagios.services will create the partition counters_detail_$service_id automatically
    					INSERT INTO wh_nagios.services (id,hostname,warehouse,service,label,seracl,unit,state,min,max,critical,warning)
    					VALUES (default,cur->'hostname','wh_nagios',cur->'servicedesc',cur->'label','{}'::aclitem[],cur->'uom',cur->'servicestate',(cur->'min')::numeric,(cur->'max')::numeric,(cur->'critical')::numeric,(cur->'warning')::numeric)
    					RETURNING id INTO STRICT service_id;
    				END IF;
    				BEGIN
    					EXECUTE format('INSERT INTO wh_nagios.counters_detail_%s (timet,value) VALUES (TIMESTAMP WITH TIME ZONE ''epoch''+%L * INTERVAL ''1 second'', %L );',service_id,cur->'timet',cur->'value');
    				EXCEPTION
    					WHEN OTHERS THEN
    				END;
                END IF;
    		END IF;
			--Delete current line
			DELETE FROM wh_nagios.hub WHERE CURRENT OF c_hub;
		END LOOP;
	EXCEPTION
		WHEN lock_not_available THEN
			--Have frendlier exception if concurrent function already running
			RAISE EXCEPTION 'Concurrent function already running.';
	END;
	RETURN true;
END;
$$;

ALTER FUNCTION wh_nagios.dispatch_record() OWNER TO pgfactory;

--Automatically create a new partition when a service is added.
CREATE OR REPLACE FUNCTION wh_nagios.create_partition_on_insert_service() RETURNS trigger
	LANGUAGE plpgsql
	AS $$
BEGIN
	EXECUTE 'CREATE TABLE wh_nagios.counters_detail_' || NEW.id || ' (timet timestamptz primary key, value numeric)';
	RETURN NEW;
EXCEPTION
	WHEN duplicate_table THEN
		EXECUTE 'TRUNCATE TABLE wh_nagios.counters_detail_' || NEW.id;
		RETURN NEW;
END;
$$;

ALTER FUNCTION wh_nagios.create_partition_on_insert_service() OWNER TO pgfactory;

--Automatically delete a partition when a service is removed.
CREATE OR REPLACE FUNCTION wh_nagios.drop_partition_on_delete_service() RETURNS trigger
	LANGUAGE plpgsql
	AS $$
BEGIN
	EXECUTE 'DROP TABLE wh_nagios.counters_detail_' || OLD.id;
	RETURN NULL;
EXCEPTION
	WHEN undefined_table THEN
		RETURN NULL;
END;
$$;

ALTER FUNCTION wh_nagios.drop_partition_on_delete_service() OWNER TO pgfactory;

CREATE TRIGGER create_partition_on_insert_service BEFORE INSERT ON wh_nagios.services FOR EACH ROW EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_service();
CREATE TRIGGER drop_partition_on_delete_service AFTER DELETE ON wh_nagios.services FOR EACH ROW EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_service();
