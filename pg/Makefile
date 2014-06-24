EXTENSION = wh_nagios
DATA = wh_nagios--2.1.sql \
	   wh_nagios--2.1--2.2.sql \
	   wh_nagios--2.2.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
