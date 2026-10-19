#  Point BSON_INCLUDES and BSON_SHLIB where the BSON development resources live
#  as installed separately before making this extension:
#  Examples:
#    OS X:  brew install mongo-c-driver
#      -I/opt/homebrew/Cellar/mongo-c-driver/1.23.2/include/libbson-1.0/bson
#      -L/opt/homebrew/Cellar/mongo-c-driver/1.23.2/lib -lbson-1.0
#
#    RH:    sudo dnf install epel-release
#           sudo yum install libbson-devel.x86_64
#           -I/usr/include/libbson-1.0/bson
#           -L/usr/lib64 -lbson-1.0
#  
#  -L only tells the linker where to find libbson at BUILD time; it does
#  NOT control where postgres finds it at RUNTIME.  Runtime lookup is the
#  dynamic loader's job:  the system library paths (e.g. /usr/lib64,
#  /usr/local/lib; ldconfig on Linux), an rpath/@rpath baked into the
#  shlib, or LD_LIBRARY_PATH / DYLD_LIBRARY_PATH in postgres' environment.
#  A build that links fine can still fail to load.  Wherever pgbson.so (or pgbson.dylib) gets installed, check with ldd (or
#  otool -L) that it knows about libbson e.g.:
#        $ ldd /usr/pgsql-16/lib/pgbson.so
#            linux-vdso.so.1 (0x00007ffc78f22000)
# good! -->  libbson-1.0.so.0 => /lib64/libbson-1.0.so.0 (0x00007fc6cfb20000)
#            libc.so.6 => /lib64/libc.so.6 (0x00007fc6cf800000)
#            /lib64/ld-linux-x86-64.so.2 (0x00007fc6cfb71000)
#
#        $ ls /lib64/libbson-1.0.so.0
#        /lib64/libbson-1.0.so.0  # found!  good!
#
#  This is the only external dependency of this extension.
#
BSON_INCLUDES = -I$(HOME)/projects/bson/include
BSON_SHLIB    = -L$(HOME)/projects/bson/lib -lbson.1


LOCAL_CFLAGS  =

# From here down is part of pgxs framework and should run anywhere.
# The variable names are very specific; do not mess with them.
MODULE_big = pgbson
EXTENSION = pgbson          # the extension's name
DATA = pgbson--2.0.sql pgbson--2.1.sql pgbson--2.2.sql pgbson--2.0--2.1.sql pgbson--2.1--2.2.sql
OBJS = pgbson.o

PG_CFLAGS = $(BSON_INCLUDES) $(LOCAL_CFLAGS)
SHLIB_LINK = $(BSON_SHLIB)

# for postgres build
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
