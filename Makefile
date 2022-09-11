#  Point this at where the BSON includes live:
BSON_INCLUDES = -I$(HOME)/projects/bson/include

#  Where the BSON lib (nominally libbson.1.dylib) can be found at RUNTIME
#  by postgres (show it with:  pg_config --libdir).  This should probably be
#  something like  /usr/local/lib  or  /usr/local/lib/postgres  but it is up
#  to you to put the shlib in the right place.  This is the only external
#  dependency of the extension.
BSON_SHLIB    = -L$(HOME)/projects/bson/lib -lbson.1

# Suppress passing vars instead of string literals in ereport errmsg:
LOCAL_CFLAGS  = -Wno-format-security

# From here down is part of pgxs framework and should run anywhere.
# The variable names are very specific; do not mess with them.
MODULE_big = pgbson
EXTENSION = pgbson          # the extension's name
DATA = pgbson--2.0.sql    # script file to install
OBJS = pgbson.o

PG_CFLAGS = $(BSON_INCLUDES) $(LOCAL_CFLAGS)
SHLIB_LINK = $(BSON_SHLIB)

# for postgres build
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

