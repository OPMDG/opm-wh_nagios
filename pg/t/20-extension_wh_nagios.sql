-- This program is open source, licensed under the PostgreSQL License.
-- For license terms, see the LICENSE file.
--
-- Copyright (C) 2012-2018: Open PostgreSQL Monitoring Development Group

\unset ECHO
\i t/setup.sql

SELECT plan(165);

CREATE OR REPLACE FUNCTION test_set_opm_session(IN p_user name)
  RETURNS SETOF TEXT LANGUAGE plpgsql AS $f$
BEGIN

    RETURN QUERY
        SELECT set_eq(
            format($$SELECT public.set_opm_session(%L)$$, p_user),
            format($$VALUES (%L)$$, p_user),
            format('Set session to "%s".', p_user)
        );

    RETURN QUERY
        SELECT results_eq(
            $$SELECT session_role()$$,
            format($$VALUES (%L::text)$$, p_user),
            format('Current OPM session is "%s".', p_user)
        );
END$f$;
SELECT diag(E'\n==== Setup environnement ====\n');

SELECT lives_ok(
    $$CREATE EXTENSION opm_core$$,
    'Create extension "opm_core"'
);

SELECT diag(E'\n==== Install wh_nagios ====\n');

SELECT throws_matching(
    $$CREATE EXTENSION wh_nagios$$,
    'required extension "hstore" is not installed',
    'Should not create extension "wh_nagios"'
);

SELECT lives_ok(
    $$CREATE EXTENSION hstore$$,
    'Create extension "hstore"'
);

SELECT lives_ok(
    $$CREATE EXTENSION wh_nagios$$,
    'Create extension "wh_nagios"'
);

SELECT has_extension(
    'wh_nagios',
    'Extension "wh_nagios" should exist.'
);

SELECT extension_schema_is('wh_nagios', 'wh_nagios',
    'Schema of extension "wh_nagios" should "wh_nagios".'
);

SELECT has_schema('wh_nagios', 'Schema "wh_nagios" should exist.');
SELECT has_table('wh_nagios', 'hub',
    'Table "hub" of schema "wh_nagios" should exists.'
);
SELECT has_table('wh_nagios', 'hub_reject',
    'Table "hub_reject" of schema "wh_nagios" should exists.'
);
SELECT has_table('wh_nagios', 'metrics',
    'Table "metrics" of schema "wh_nagios" should exists.'
);
SELECT has_table('wh_nagios', 'series',
    'Table "series" of schema "wh_nagios" should exists.'
);
SELECT has_table('wh_nagios', 'services',
    'Table "services" of schema "wh_nagios" should exists.'
);
SELECT has_view('wh_nagios', 'services_metrics',
    'View "services_metrics" of schema "wh_nagios" should exists.'
);

-- All tables/sequences should be dumped by pg_dump
SELECT set_eq(
    $$
    WITH dumped AS (SELECT unnest(extconfig) AS oid
            FROM pg_extension
                WHERE extname = 'wh_nagios'
            ),
            ext AS (SELECT c.oid,c.relname
                FROM pg_depend d
                JOIN pg_extension e ON d.refclassid = (SELECT oid FROM pg_class WHERE relname = 'pg_extension') AND d.refobjid = e.oid AND d.deptype = 'e'
                JOIN pg_class c ON d.objid = c.oid AND c.relkind in ('S','r')
                WHERE e.extname = 'wh_nagios'
            )
            SELECT count(*) FROM ext
            LEFT JOIN dumped ON dumped.oid = ext.oid
            WHERE dumped.oid IS NULL;
    $$,
    $$ VALUES (0) $$,
    'All tables and sequences should be dumped by pg_dump.'
);

SELECT has_function('wh_nagios', 'cleanup_service', '{bigint}', 'Function "wh_nagios.cleanup_service" should exists.');
SELECT has_function('wh_nagios', 'create_partition_on_insert_service', '{}', 'Function "wh_nagios.create_partition_on_insert_service" should exists.');
SELECT has_function('wh_nagios', 'delete_metrics', '{bigint[]}', 'Function "wh_nagios.delete_metrics" should exists.');
SELECT has_function('wh_nagios', 'delete_services', '{bigint[]}', 'Function "wh_nagios.delete_services" should exists.');
SELECT has_function('wh_nagios', 'dispatch_record', '{integer,boolean}', 'Function "wh_nagios.dispatch_record" should exists.');
SELECT has_function('wh_nagios', 'drop_partition_on_delete_service', '{}', 'Function "wh_nagios.create_partition_on_insert_service" should exists.');
SELECT has_function('wh_nagios', 'get_metric_data', '{bigint, timestamp with time zone, timestamp with time zone}', 'Function "wh_nagios.get_metric_data" (bigint, timestamp with time zone, timestamp with time zone) should exists.');
SELECT has_function('wh_nagios', 'get_metric_timespan', '{bigint}', 'Function "wh_nagios.get_metric_timespan" (bigint) should exists.');
SELECT has_function('wh_nagios', 'grant_dispatcher', '{name}', 'Function "wh_nagios.grant_dispatcher" should exists.');
SELECT has_function('wh_nagios', 'list_metrics', '{bigint}', 'Function "wh_nagios.list_metrics" should exists.');
SELECT has_function('wh_nagios', 'list_services', '{}', 'Function "wh_nagios.list_services" should exists.');
SELECT has_function('wh_nagios', 'merge_service', '{bigint, bigint, boolean}', 'Function "wh_nagios.merge_service" should exists.');
SELECT has_function('wh_nagios', 'purge_services', '{bigint[]}', 'Function "wh_nagios.purge_services" should exists.');
SELECT has_function('wh_nagios', 'revoke_dispatcher', '{name}', 'Function "wh_nagios.revoke_dispatcher" should exists.');
SELECT has_function('wh_nagios', 'update_services_validity', '{interval, bigint[]}', 'Function "wh_nagios.update_services_validity" should exists.');

SELECT has_trigger('wh_nagios', 'services', 'create_partition_on_insert_service', 'Trigger "create_partition_on_insert_service" on table "wh_nagios.services" should exists.');
SELECT has_trigger('wh_nagios', 'services', 'drop_partition_on_delete_service', 'Trigger "drop_partition_on_delete_service" on table "wh_nagios.services" should exists.');



SELECT diag(E'\n==== Test wh_nagios functions ====\n');

SELECT set_eq(
    $$SELECT * FROM public.create_admin('opmtestadmin','opmtestadmin')$$,
    $$VALUES (2, 'opmtestadmin')$$,
    'Account "opmtestadmin" should be created.'
);

SELECT set_eq(
    $$SELECT public.authenticate('opmtestadmin', md5('opmtestadminopmtestadmin'))$$,
    $$VALUES (true)$$,
    'Authenticate "opmtestadmin".'
);


SELECT test_set_opm_session('opmtestadmin');

SELECT diag(
    'Create account: ' || public.create_account('acc1') ||
    E'\nCreate user: ' || public.create_user('u1', 'pass', '{acc1}') ||
    E'\n'
);

SELECT test_set_opm_session('u1');

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.list_metrics(1)$$,
    $$VALUES (0)$$,
    'User "u1" should have access to list_metrics function in "wh_nagios".'
);

-- grant dispatching to dispatch1
SELECT lives_ok(
    $$CREATE USER dispatch1 WITH PASSWORD 'dispatch1'$$,
    'Create user dispatch1'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.grant_dispatcher('dispatch1')$$,
    $$VALUES ('GRANT','dispatch1','USAGE','SCHEMA','wh_nagios'),
      ('GRANT','dispatch1','USAGE','SEQUENCE','wh_nagios.hub_id_seq'),
      ('GRANT','dispatch1','INSERT','TABLE','wh_nagios.hub'),
      ('GRANT','dispatch1','EXECUTE','FUNCTION','wh_nagios.dispatch_record(integer, bool)'),
      ('GRANT','dispatch1','CONNECT','DATABASE','opm')$$,
    'User "dispatch1" should have been granted to dispatch in "wh_nagios".'
);

SELECT lives_ok(
    $$GRANT USAGE ON SCHEMA public TO dispatch1$$,
    'Allow user "dispatch1" to use pgTap'
);

SELECT schema_privs_are('public', 'dispatch1', '{USAGE}',
    'Role "dispatch1" should only have priv "USAGE" on schema "public".'
);
SELECT schema_privs_are('wh_nagios', 'dispatch1', '{USAGE}',
    'Role "dispatch1" should only have priv "USAGE" on schema "wh_nagios".'
);
SELECT table_privs_are('wh_nagios', 'hub', 'dispatch1', '{INSERT}',
    'Role "dispatch1" should only have priv "INSERT" on table "wh_nagios.hub".'
);
SELECT sequence_privs_are('wh_nagios', 'hub_id_seq', 'dispatch1', '{USAGE}',
    'Role "dispatch1" should only have priv "USAGE" on sequence "wh_nagios.hub_id_seq".'
);

SELECT table_privs_are('wh_nagios', 'metrics', 'dispatch1', '{}',
    'Role "dispatch1" should not have right on table "wh_nagios.metrics".'
);
SELECT table_privs_are('wh_nagios', 'series', 'dispatch1', '{}',
    'Role "dispatch1" should not have right on table "wh_nagios.series".'
);
SELECT table_privs_are('wh_nagios', 'services', 'dispatch1', '{}',
    'Role "dispatch1" should not have right on table "wh_nagios.services".'
);

SELECT diag(E'\n==== Test dispatching ====\n');

-- inserting data with role dispatch1.
SET ROLE dispatch1;

SELECT results_eq(
    $$SELECT current_user$$,
    $$VALUES ('dispatch1'::name)$$,
    'Set role to dispatch1'
);

SELECT lives_ok($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (1, ARRAY[ -- more than one dim
            ['BAD RECORD'], ['BAD RECORD'],
            ['BAD RECORD'], ['BAD RECORD'],
            ['BAD RECORD'], ['BAD RECORD'],
            ['BAD RECORD'], ['BAD RECORD'],
            ['BAD RECORD'], ['BAD RECORD']
        ]),
        (2, ARRAY['BAD RECORD', 'ANOTHER ONE']), -- less than 10 values
        (3, ARRAY[ -- number of parameter not even
            'BAD RECORD', 'BAD RECORD',
            'BAD RECORD', 'BAD RECORD',
            'BAD RECORD', 'BAD RECORD',
            'BAD RECORD', 'BAD RECORD',
            'BAD RECORD', 'BAD RECORD',
            'BAD RECORD'
        ]),
        (4, ARRAY[ -- missing hostname
            'SERVICEDESC','pgactivity Database size',
            'LABEL','template0',
            'TIMET','1357208343',
            'VALUE','5284356',
            'SERVICESTATE','OK'
        ]),
        (5, ARRAY[ -- missing service desc
            'HOSTNAME','server1',
            'LABEL','template0',
            'TIMET','1357208343',
            'VALUE','5284356',
            'SERVICESTATE','OK'
        ]),
        (6, ARRAY[ -- missing label
            'HOSTNAME','server1',
            'SERVICEDESC','pgactivity Database size',
            'TIMET','1357208343',
            'VALUE','5284356',
            'SERVICESTATE','OK'
        ]),
        (7, ARRAY[ -- missing timet
            'HOSTNAME','server1',
            'SERVICEDESC','pgactivity Database size',
            'LABEL','template0',
            'VALUE','5284356',
            'SERVICESTATE','OK'
        ]),
        (8, ARRAY[ -- missing value
            'HOSTNAME','server1',
            'SERVICEDESC','pgactivity Database size',
            'LABEL','template0',
            'TIMET','1357208343',
            'SERVICESTATE','OK'
        ]),
        (9, ARRAY[ -- good one
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (10, ARRAY[ -- another good one
            'MIN','0',
            'WARNING','209715200',
            'VALUE','6284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','B',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (11, ARRAY[ -- another good one
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7284356',
            'CRITICAL','524288000',
            'LABEL','postgres',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','B',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','pgactivity Database size'
        ]
    )$$,
    'Insert some datas in "wh_nagios.hub" with role "dispatch1".'
);

RESET ROLE;
SELECT results_ne(
    $$SELECT current_user$$,
    $$VALUES ('dispatch1'::name)$$,
    'Reset role.'
);

SELECT test_set_opm_session('opmtestadmin');

SELECT diag(E'');
SELECT diag('inserted record: ' || s)
FROM wh_nagios.hub AS s;
SELECT diag(E'');

-- dispatching records
SELECT results_eq(
    $$SELECT * FROM wh_nagios.dispatch_record(10000,true)$$,
    $$VALUES (3::bigint,8::bigint)$$,
    'Dispatching records.'
);

-- check rejected lines and status
SELECT set_eq(
    $$SELECT id, rolname, msg FROM wh_nagios.hub_reject$$,
    $$VALUES (1::bigint, 'dispatch1'::name, 'given array has more than 1 dimension'),
        (2, 'dispatch1', 'less than 10 values'),
        (3, 'dispatch1', 'number of parameter not even'),
        (4, 'dispatch1', 'hostname required'),
        (5, 'dispatch1', 'servicedesc required'),
        (6, 'dispatch1', 'label required'),
        (7, 'dispatch1', 'timet required'),
        (8, 'dispatch1', 'value required')$$,
    'Checking rejected lines.'
);

-- check table hub is now empty
SELECT set_eq(
    $$SELECT count(*) FROM wh_nagios.hub$$,
    $$VALUES (0::bigint)$$,
    'Table "wh_nagios.hub" should be empty now.'
);

-- check table wh_nagios.service_counters_1
SELECT has_table('wh_nagios', 'service_counters_1',
    'Table "wh_nagios.service_counters_1" should exists.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.list_metrics(1)$$,
    $$VALUES (1::bigint, 'template0', '', 0::numeric, 0::numeric, 524288000::numeric, 209715200::numeric)$$,
    'list_metrics should see metric template0.'
);

SELECT set_eq(
    $$SELECT date_records, extract(epoch FROM (c.records[1]).timet),
            (c.records[1]).value
        FROM wh_nagios.service_counters_1 AS c$$,
    $$VALUES ('2013-01-01'::date, 1357038000::double precision,
        5284356::numeric)$$,
    'Table "wh_nagios.service_counters_1" should have value of record 9.'
);

-- check table wh_nagios.counters_detail_2
SELECT has_table('wh_nagios', 'service_counters_2',
    'Table "wh_nagios.service_counters_2" should exists.'
);

SELECT set_eq(
    $$SELECT date_records, extract(epoch FROM (c.records[1]).timet),
            (c.records[1]).value
        FROM wh_nagios.service_counters_2 AS c$$,
    $$VALUES
        ('2013-01-01'::date, 1357038000::double precision, 6284356::numeric),
        ('2013-01-01'::date, 1357038000::double precision, 7284356::numeric)$$,
    'Table "wh_nagios.service_counters_2" should have value of record 10 and 11.'
);

-- check table public.services
SELECT set_eq(
    $$SELECT s1.id, s2.hostname, s1.warehouse, s1.service, s1.last_modified, s1.creation_ts,
            s1.last_cleanup, s1.servalid, s2.id_role
        FROM public.services s1 JOIN public.servers s2 ON s1.id_server = s2.id$$,
    $$VALUES
        (1::bigint, 'server1', 'wh_nagios'::name,
            'pgactivity Database size', current_date, now(), now(),
            NULL::interval, NULL::bigint),
        (2::bigint, 'server2', 'wh_nagios'::name,
            'pgactivity Database size', current_date, now(), now(),
            NULL::interval, NULL::bigint)$$,
    'Table "public.services" should have services defined by records 9, 10 (and 11).'
);

-- check table wh_nagios.services
SELECT set_eq(
    $$SELECT s1.id, s2.hostname, s1.warehouse, s1.service, s1.last_modified, s1.creation_ts,
            s1.last_cleanup, s1.servalid, s2.id_role, s1.state, l.min::numeric,
            l.max::numeric, l.critical::numeric, l.warning::numeric,
            extract(epoch FROM s1.oldest_record) AS oldest_record,
            extract(epoch FROM s1.newest_record) AS newest_record
        FROM wh_nagios.services s1
        JOIN wh_nagios.metrics l ON s1.id = l.id_service
        JOIN public.servers s2 ON s1.id_server = s2.id$$,
    $$VALUES
        (1::bigint, 'server1', 'wh_nagios'::name,
            'pgactivity Database size', current_date, now(), now(),
            NULL::interval, NULL::bigint, 'OK', 0, 0, 524288000,
            209715200, 1357038000::double precision, 1357038000::double precision),
        (2::bigint, 'server2', 'wh_nagios'::name,
            'pgactivity Database size', current_date, now(), now(),
            NULL::interval, NULL::bigint, 'OK', 0, 0, 524288000,
            209715200, 1357038000::double precision, 1357038000::double precision)$$,
    'Table "wh_nagios.services" should have services defined by records 9, 10 (and 11).'
);

-- check table public.metrics
SELECT set_eq(
    $$SELECT * FROM wh_nagios.metrics$$,
    $$VALUES
        (1,1,'template0', '', '{}'::text[], 0::numeric, 0::numeric,
            524288000::numeric, 209715200::numeric),
        (2,2,'template0', 'B', '{}'::text[],  0::numeric, 0::numeric,
            524288000::numeric, 209715200::numeric),
        (3,2,'postgres', 'B', '{}'::text[],  0::numeric, 0::numeric,
            524288000::numeric, 209715200::numeric)$$,
    'Table "wh_nagios.metrics" should contains metrics of records 9, 10 and 11.'
);

-- Revoke dispatching to dispatch1
SELECT set_eq(
    $$SELECT * FROM wh_nagios.revoke_dispatcher('dispatch1')$$,
    $$VALUES ('REVOKE','dispatch1','ALL','SCHEMA','wh_nagios'),
      ('REVOKE','dispatch1','ALL','SEQUENCE','wh_nagios.hub_id_seq'),
      ('REVOKE','dispatch1','ALL','TABLE','wh_nagios.hub'),
      ('REVOKE','dispatch1','ALL','DATABASE','opm')$$,
    'Revoke dispatch in "wh_nagios" from role "u1".'
);

SELECT table_privs_are('wh_nagios', 'hub', 'dispatch1', '{}',
    'Role "dispatch1" should not have privs on table "wh_nagios.hub".'
);
SELECT sequence_privs_are('wh_nagios', 'hub_id_seq', 'dispatch1', '{}',
    'Role "dispatch1" should not have privs on sequence "wh_nagios.hub_id_seq".'
);

-- test inserting with dispatch1
SET ROLE dispatch1;
SELECT results_eq(
    $$SELECT current_user$$,
    $$VALUES ('dispatch1'::name)$$,
    'Set role to dispatch1.'
);


SELECT throws_matching($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (12, ARRAY[ -- a good one
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7284356',
            'CRITICAL','524288000',
            'LABEL','postgres',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','pgactivity Database size'
        ]
    )$$,
    'permission denied',
    'Insert now fail on "wh_nagios.hub" with role "dispatch1".'
);

RESET ROLE;
SELECT results_ne(
    $$SELECT current_user$$,
    $$VALUES ('u1'::name)$$,
    'Reset role.'
);

SELECT lives_ok($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (1, ARRAY[ -- unit is now "b"
            'MIN','0',
            'WARNING','209715200',
            'VALUE','6284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','b',
            'SERVICESTATE','OK',
            'TIMET','1357638000',
            'SERVICEDESC','pgactivity Database size'
        ])$$,
    'Insert some datas in "wh_nagios.hub" and change unit.'
);

SELECT results_eq(
    $$SELECT * FROM wh_nagios.dispatch_record(10000,true)$$,
    $$VALUES (1::bigint,0::bigint)$$,
    'Dispatching the record.'
);

-- check table wh_nagios.services
SELECT set_eq(
    $$SELECT l.unit
        FROM wh_nagios.metrics l
        JOIN wh_nagios.services s1 ON l.id_service = s1.id
        JOIN public.servers s2 ON s1.id_server = s2.id
        WHERE s2.hostname = 'server2'
            AND service = 'pgactivity Database size'
            AND label = 'template0'$$,
    $$VALUES ('b')$$,
    'Field "unit" in "wh_nagios.metrics" should be "b" instead of "B".'
);

SELECT diag(E'\n==== Partition cleanup ====\n');

SELECT lives_ok($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (13, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038300',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (14, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038600',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (15, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038900',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (16, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357039200',
            'SERVICEDESC','pgactivity Database size'
        ]),
        (17, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7285356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357039500',
            'SERVICEDESC','pgactivity Database size'
        ]
    )$$,
    'Insert some more values for service 1, label "template0"'
);

-- dispatching new records
SELECT set_eq(
    $$SELECT * FROM wh_nagios.dispatch_record(10000,true)$$,
    $$VALUES (5::bigint,0::bigint)$$,
    'Dispatching 5 new records.'
);

SELECT set_eq(
    $$WITH u AS (UPDATE wh_nagios.services
            SET last_cleanup = oldest_record - INTERVAL '1 month'
            RETURNING last_cleanup
        )
        SELECT * FROM u$$,
    $$VALUES (to_timestamp(1357038000) - INTERVAL '1 month')$$,
    'Set a fake last_cleanup timestamp.'
);

SELECT set_eq(
    $$SELECT wh_nagios.cleanup_service(1)$$,
    $$VALUES (true)$$,
    'Run cleanup_service on service 1.'
);

SELECT set_eq(
    $$SELECT last_cleanup, extract(epoch FROM oldest_record) AS oldest_record,
            extract(epoch FROM newest_record) AS newest_record
        FROM wh_nagios.services
        WHERE id=1$$,
    $$VALUES (now(), 1357038000::double precision,
        1357039500::double precision)$$,
    'Table "wh_nagios.services" fields should reflect last cleanup activity.'
);

-- add some new datas
SELECT lives_ok($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (13, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','5284356',
            'CRITICAL','524288000',
            'LABEL','template0',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038300',
            'SERVICEDESC','pgactivity Database size'
        ])$$,
    'Insert some more values for service 1, label "template0"'
);

SELECT set_eq(
    $$WITH u AS (UPDATE wh_nagios.services
            SET last_cleanup = oldest_record - INTERVAL '1 month'
        WHERE service = 'pgactivity Database size'
            RETURNING last_cleanup
        )
        SELECT * FROM u$$,
    $$VALUES (to_timestamp(1357038000) - INTERVAL '1 month')$$,
    'Set a fake last_cleanup timestamp.'
);

-- dispatching new records
SELECT set_eq(
    $$SELECT * FROM wh_nagios.dispatch_record(10000,true)$$,
    $$VALUES (1::bigint,0::bigint)$$,
    'Dispatching 1 new records.'
);

-- verify that wh_nagios.dispatch_record() called also the cleanup() function
SELECT set_eq(
    $$SELECT last_cleanup, extract(epoch FROM oldest_record) AS oldest_record,
            extract(epoch FROM newest_record) AS newest_record
        FROM wh_nagios.services
        WHERE id=1$$,
    $$VALUES (now(), 1357038000::double precision,
        1357039500::double precision)$$,
    'Table "wh_nagios.services" fields should reflect last cleanup activity.'
);

SELECT diag('counters: '|| s) FROM wh_nagios.service_counters_1 AS s;
SELECT set_eq(
    $$SELECT date_records, extract(epoch FROM timet), value
        FROM (SELECT date_records, (unnest(records)).*
            FROM wh_nagios.service_counters_1
        ) as t$$,
    $$VALUES
        ('2013-01-01'::date, 1357038000::double precision, 5284356::numeric),
        ('2013-01-01'::date, 1357039200, 5284356),
        ('2013-01-01'::date, 1357039500, 7285356),
        ('2013-01-01'::date, 1357038300, 5284356),
        ('2013-01-01'::date, 1357038600, 5284356),
        ('2013-01-01'::date, 1357038900, 5284356)
        $$,
    'Consecutive records with same value of "wh_nagios.service_counters_1" should not be cleaned.'
);

SELECT set_eq(
    $$SELECT date_records FROM wh_nagios.service_counters_1$$,
    $$VALUES
        ('2013-01-01'::date)
    $$,
    'Records of "wh_nagios.service_counters_1" should be aggregated.'
);

SELECT diag(E'\n==== Updating a service validity ====\n');

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.update_services_validity('2 days',999,null,1024)$$,
    $$VALUES (0)$$,
    'Updating unexisting services should not return any service.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.services
    WHERE servalid = '2 days'$$,
    $$VALUES (0)$$,
    'No service should be updated.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.update_services_validity('2 days', 1)$$,
    $$VALUES (1)$$,
    'Update one service.'
);

SELECT set_eq(
    $$SELECT id, servalid FROM wh_nagios.services$$,
    $$VALUES (1::bigint, '2 days'::interval),
    (2, null)$$,
    'Only service 1 should be updated.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.update_services_validity('5 days', 1, 2)$$,
    $$VALUES (1),(2)$$,
    'Update two services.'
);

SELECT set_eq(
    $$SELECT id, servalid FROM wh_nagios.services$$,
    $$VALUES (1::bigint,'5 days'::interval),
    (2, '5 days')$$,
    'Service 1 and 2  should be updated.'
);


SELECT diag(E'\n==== Dropping a service ====\n');

SELECT lives_ok(
    $$DELETE FROM wh_nagios.services WHERE id = 2$$,
    'Delete service with id=2.'
);

-- check table public.labels do not have label from service 2 anymore
SELECT set_eq(
    $$SELECT * FROM wh_nagios.metrics$$,
    $$VALUES (1::bigint,1::bigint,'template0', '', '{}'::text[],0::numeric, 0::numeric,
        524288000::numeric, 209715200::numeric)$$,
    'Table "wh_nagios.metrics" should not contains labels of service id 2 anymore.'
);

-- check tables has been drop'ed
SELECT hasnt_table('wh_nagios', 'service_counters_2',
    'Table "wh_nagios.service_counters_2" should not exists anymore.'
);

SELECT diag(E'\n==== Check privileges ====\n');

-- schemas privs
SELECT schema_privs_are('wh_nagios', 'public', ARRAY[]::name[]);

-- tables privs
SELECT table_privs_are('wh_nagios', c.relname, 'public', ARRAY[]::name[])
FROM pg_catalog.pg_class c
WHERE c.relkind = 'r'
    AND c.relnamespace = (
        SELECT oid FROM pg_catalog.pg_namespace n WHERE nspname = 'wh_nagios'
    )
    AND c.relpersistence <> 't';

-- sequences privs
SELECT sequence_privs_are('wh_nagios', c.relname, 'public', ARRAY[]::name[])
FROM pg_catalog.pg_class c
WHERE c.relkind = 'S'
    AND c.relnamespace = (
        SELECT oid FROM pg_catalog.pg_namespace n WHERE nspname = 'wh_nagios'
    )
    AND c.relpersistence <> 't';

-- functions privs
SELECT function_privs_are( n.nspname, p.proname, (
        SELECT string_to_array(oidvectortypes(proargtypes), ', ')
        FROM pg_proc
        WHERE oid=p.oid
    ),
    'public', ARRAY[]::name[]
)
FROM pg_depend dep
    JOIN pg_catalog.pg_proc p ON dep.objid = p.oid
    JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
WHERE dep.deptype= 'e'
    AND dep.refobjid = (
        SELECT oid FROM pg_extension WHERE extname = 'wh_nagios'
    );


SELECT diag(E'\n==== Dropping a metric ====\n');

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.delete_metrics(-1)$$,
    $$VALUES (0)$$,
    'Deleting an unexisting metric should not return any metic id.'
);

SELECT set_eq(
    $$SELECT id FROM wh_nagios.metrics$$,
    $$VALUES (1::bigint)$$,
    'All metrics should still be there.'
);

SELECT set_eq(
    $$SELECT relname FROM pg_class WHERE relkind = 'r' AND relname ~ '^service_counters'$$,
    $$VALUES ('service_counters_1')$$,
    'All partitions related to metrics should still be there.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.delete_metrics(1)$$,
    $$VALUES (1)$$,
    'Delete a metric should return the id of the metric.'
);

SELECT set_eq(
    $$SELECT count(*) FROM wh_nagios.metrics$$,
    $$VALUES (0)$$,
    'Metric 1 should be deleted.'
);

-- currently fails
SELECT set_eq(
    $$SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relname ~ '^service_counters'$$,
    $$VALUES (0)$$,
    'Partition related to metric 1 should be dropped.'
);


SELECT diag(E'\n==== Merging services ====\n');

SELECT lives_ok($$
    INSERT INTO wh_nagios.hub (id, data) VALUES
        (1, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','1',
            'CRITICAL','524288000',
            'LABEL','val',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','Test merge src'
        ]),
        (2, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','6284356',
            'CRITICAL','524288000',
            'LABEL','val',
            'HOSTNAME','server1',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357124400',
            'SERVICEDESC','Test merge dst'
        ]),
        (3, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7284356',
            'CRITICAL','524288000',
            'LABEL','val',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357038000',
            'SERVICEDESC','Test merge other server 1'
        ]),
        (4, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7284356',
            'CRITICAL','524288000',
            'LABEL','val',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','B',
            'SERVICESTATE','OK',
            'TIMET','1357124400',
            'SERVICEDESC','Test merge other server 2'
        ]),
        (5, ARRAY[
            'MIN','0',
            'WARNING','209715200',
            'VALUE','7284356',
            'CRITICAL','524288000',
            'LABEL','val2',
            'HOSTNAME','server2',
            'MAX','0',
            'UOM','',
            'SERVICESTATE','OK',
            'TIMET','1357124400',
            'SERVICEDESC','Test merge other server 3'
        ]
    )$$,
    'Insert some datas for merging in "wh_nagios.hub".'
);

SELECT results_eq(
    $$SELECT * FROM wh_nagios.dispatch_record(10000,true)$$,
    $$VALUES (5::bigint,0::bigint)$$,
    'Dispatch the 5 records.'
);

SELECT set_eq(
    $$SELECT id,service,extract(epoch FROM oldest_record) AS old, extract(epoch FROM newest_record) AS new FROM wh_nagios.services WHERE id IN (3,4,5,6,7)$$,
    $$VALUES (3::bigint,'Test merge src',1357038000,1357038000),
    (4,'Test merge dst',1357124400,1357124400),
    (5,'Test merge other server 1',1357038000,1357038000),
    (6,'Test merge other server 2',1357124400,1357124400),
    (7,'Test merge other server 3',1357124400,1357124400)
    $$,
    'Services "Test merge src", "Test merge dst" and "Test merge other server x" should have been created.'
);

-- SELECT diag('wh_nagios: '|| relname) FROM pg_class AS s WHERE relname ~ '^service_coun';

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.service_counters_4$$,
    $$VALUES (1)$$,
    'Partition table related to service "Test merge src" should only contains 1 row.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(-1, -2)$$,
    $$VALUES (false)$$,
    'Merging two unexisting services should return false.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(-1, 1)$$,
    $$VALUES (false)$$,
    'Merging an unexisting service with an existing one should return false.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(3, 5)$$,
    $$VALUES (false)$$,
    'Merging an two services from different servers should return false.'
);

SAVEPOINT merge ;

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(3, 4)$$,
    $$VALUES (true)$$,
    'Merging two services should return true.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.services WHERE service = 'Test merge src'$$,
    $$VALUES (1)$$,
    'Merging two services without deleting source should not delete source.'
);

ROLLBACK TO merge ;

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(3, 4, true)$$,
    $$VALUES (true)$$,
    'Merging two services should return true.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.services WHERE service = 'Test merge src'$$,
    $$VALUES (0)$$,
    'Merging two services with deleting source should delete source.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.metrics m JOIN wh_nagios.services s ON s.id = m.id_service WHERE s.service = 'Test merge dst'$$,
    $$VALUES (1)$$,
    'Merging two services with an identical metric should not duplicate it.'
);

SELECT set_eq(
    $$SELECT id,service,extract(epoch FROM oldest_record) AS old, extract(epoch FROM newest_record) AS new FROM wh_nagios.services WHERE id = 4$$,
    $$VALUES (4,'Test merge dst',1357038000,1357124400)$$,
    'Metadata from services "Test merge dst" should have been updated.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.service_counters_4$$,
    $$VALUES (2)$$,
    'Partition table related to service "Test merge src" should now contains 2 rows.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(6, 5, true)$$,
    $$VALUES (true)$$,
    'Merging two services whith different units should return true.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.metrics m JOIN wh_nagios.services s ON s.id = m.id_service WHERE s.service = 'Test merge other server 1'$$,
    $$VALUES (2)$$,
    'Merging two services with different units should create a new metric.'
);

SELECT set_eq(
    $$SELECT * FROM wh_nagios.merge_service(7, 5, true)$$,
    $$VALUES (true)$$,
    'Merging two services whith different metric name should return true.'
);

SELECT set_eq(
    $$SELECT COUNT(*) FROM wh_nagios.metrics m JOIN wh_nagios.services s ON s.id = m.id_service WHERE s.service = 'Test merge other server 1'$$,
    $$VALUES (3)$$,
    'Merging two services whith different metric name should create a new metric.'
);


SELECT diag(E'\n==== Drop wh_nagios ====\n');

SELECT lives_ok(
    $$DROP EXTENSION wh_nagios CASCADE;$$,
    'Drop extension "wh_nagios"'
);

SELECT hasnt_table('wh_nagios', 'hub',
    'Table "hub" of schema "wh_nagios" should not exists anymore.'
);
SELECT hasnt_table('wh_nagios', 'hub_reject',
    'Table "hub_reject" of schema "wh_nagios" should not exists anymore.'
);
SELECT hasnt_table('wh_nagios', 'metrics',
    'Table "metrics" of schema "wh_nagios" should not exists anymore.'
);
SELECT hasnt_table('wh_nagios', 'series',
    'Table "series" of schema "wh_nagios" should not exists anymore.'
);
SELECT hasnt_table('wh_nagios', 'services',
    'Table "services" of schema "wh_nagios" should not exists anymore.'
);
SELECT hasnt_view('wh_nagios', 'services_metrics',
    'View "services_metrics" of schema "wh_nagios" should not exists anymore.'
);

SELECT hasnt_function('wh_nagios', 'cleanup_service', '{bigint}', 'Function "wh_nagios.cleanup_service" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'create_partition_on_insert_metric', '{}', 'Function "wh_nagios.create_partition_on_insert_metric" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'delete_metrics', '{bigint[]}', 'Function "wh_nagios.delete_metrics" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'delete_services', '{bigint[]}', 'Function "wh_nagios.delete_services" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'dispatch_record', '{integer,boolean}', 'Function "wh_nagios.dispatch_record" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'drop_partition_on_delete_metric', '{}', 'Function "wh_nagios.create_partition_on_insert_metric" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'get_metric_data', '{bigint, timestamp with time zone, timestamp with time zone}', 'Function "wh_nagios.get_metric_data" (bigint, timestamp with time zone, timestamp with time zone) should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'get_metric_timespan', '{bigint}', 'Function "wh_nagios.get_metric_timespan" (bigint) should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'grant_dispatcher', '{name}', 'Function "wh_nagios.grant_dispatcher" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'list_metrics', '{bigint}', 'Function "wh_nagios.list_metrics" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'list_services', '{}', 'Function "wh_nagios.list_services" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'merge_service', '{bigint, bigint, boolean}', 'Function "wh_nagios.merge_service" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'purge_services', '{bigint[]}', 'Function "wh_nagios.purge_services" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'revoke_dispatcher', '{name}', 'Function "wh_nagios.revoke_dispatcher" should not exists anymore.');
SELECT hasnt_function('wh_nagios', 'update_services_validity', '{interval, bigint[]}', 'Function "wh_nagios.update_services_validity" should not exists anymore.');

SELECT hasnt_trigger('wh_nagios', 'metrics', 'create_partition_on_insert_metric', 'Trigger "create_partition_on_insert_metric" on table "wh_nagios.metrics" should not exists anymore.');
SELECT hasnt_trigger('wh_nagios', 'metrics', 'drop_partition_on_delete_metric', 'Trigger "drop_partition_on_delete_metric" on table "wh_nagios.metrics" should not exists anymore.');

-- Finish the tests and clean up.
SELECT * FROM finish();

ROLLBACK;
