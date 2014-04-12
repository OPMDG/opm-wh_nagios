EXTENSION = wh_nagios
DATA = wh_nagios--2.0.sql \
	   wh_nagios--2.1.sql \
       wh_nagios--1.1--2.0.sql \
	   wh_nagios--2.0--2.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
