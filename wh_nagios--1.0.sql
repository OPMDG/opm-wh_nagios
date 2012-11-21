-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION wh_nagios" to load this file. \quit

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

