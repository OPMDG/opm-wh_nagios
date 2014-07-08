EXTENSION = wh_nagios
DATA = wh_nagios--2.2.sql \
	   wh_nagios--2.2--2.3.sql \
	   wh_nagios--2.3.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
