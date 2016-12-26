EXTENSION = wh_nagios
DATA = $(wildcard wh_nagios*--*.sql)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
