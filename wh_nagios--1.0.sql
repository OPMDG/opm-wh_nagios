-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

CREATE TABLE wh_nagios.hub (
	id bigserial,
	data text[]
);

REVOKE ALL ON TABLE wh_nagios.hub FROM public ;

CREATE TABLE wh_nagios.services (
	unit		text,
	state		text,
	min			numeric,
	max			numeric,
	critical	numeric,
	warning		numeric
)
INHERITS (public.services);

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
BEGIN
/*
TODO: Handle seracl 

*/
	BEGIN
		SET search_path to wh_nagios;
		FOR r_hub IN c_hub LOOP
			/*Specific structure key-value:
			1-2: min
			3-4: warning
			5-6: value
			7-8: critical
			9-10: label
			11-12: hostname
			13-14: max
			15-16: unit
			17-18: state
			19-20: timet
			21-22: service
			*/
			IF (array_upper(r_hub.data,1) = 22) THEN
				--Does the service exists ?
				SELECT id INTO STRICT service_id
				FROM wh_nagios.services
				WHERE hostname = r_hub.data[12]
					AND service = r_hub.data[22]
					AND label = r_hub.data[10];
				
				IF NOT FOUND THEN
					-- The trigger on wh_nagios.services will create the partition counters_detail_$service_id automatically
					INSERT INTO wh_nagios.services (id,hostname,warehouse,service,label,seracl,unit,state,min,max,critical,warning)
					VALUES (default,r_hub.data[12],'wh_nagios',r_hub.data[22],r_hub.data[10],'{}'::aclitem[],r_hub.data[16],r_hub.data[18],r_hub.data[2]::numeric,r_hub.data[14]::numeric,r_hub.data[8]::numeric,r_hub.data[4]::numeric)
					RETURNING id INTO STRICT service_id;
				END IF;
				BEGIN
					EXECUTE format('INSERT INTO wh_nagios.counters_detail_%s (timet,value) VALUES (TIMESTAMP WITH TIME ZONE ''epoch''+%L * INTERVAL ''1 second'', %L );',service_id,r_hub.data[20],r_hub.data[6]);
				EXCEPTION
					WHEN OTHERS THEN
				END;
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


CREATE TRIGGER create_partition_on_insert_service BEFORE INSERT ON wh_nagios.services FOR EACH ROW EXECUTE PROCEDURE wh_nagios.create_partition_on_insert_service();
CREATE TRIGGER drop_partition_on_delete_service AFTER DELETE ON wh_nagios.services FOR EACH ROW EXECUTE PROCEDURE wh_nagios.drop_partition_on_delete_service();
