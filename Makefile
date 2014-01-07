EXTENSION = wh_nagios
DATA = wh_nagios--1.0.sql wh_nagios--1.0--1.1.sql \
	wh_nagios--1.1--1.0.sql wh_nagios--1.1.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
