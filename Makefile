#  libbson is the only external dependency of this extension.  Install its
#  development package first, e.g.:
#    Fedora/RHEL:  sudo dnf install libbson-devel      (EPEL on RHEL)
#    OS X:         brew install mongo-c-driver@1
#
#  By default, pkg-config finds it:  "bson2" (libbson 2.x) if installed,
#  else "libbson-1.0".  The build prints which one it picked; check that line.
#  If pkg-config cannot see it (e.g. brew's keg-only mongo-c-driver@1), tell it
#  where to look:
#    PKG_CONFIG_PATH=/opt/homebrew/opt/mongo-c-driver@1/lib/pkgconfig make
#
#  Or skip pkg-config entirely by setting BOTH of these yourself, on the make
#  command line or in the environment.  The include dir is the one ABOVE bson/:
#    make BSON_INCLUDES=-I/usr/include/libbson-1.0 BSON_SHLIB="-L/usr/lib64 -lbson-1.0"
#
#  -L only tells the linker where to find libbson at BUILD time; it does
#  NOT control where postgres finds it at RUNTIME.  Runtime lookup is the
#  dynamic loader's job:  the system library paths (e.g. /usr/lib64,
#  /usr/local/lib; ldconfig on Linux), an rpath/@rpath baked into the
#  shlib, or LD_LIBRARY_PATH / DYLD_LIBRARY_PATH in postgres' environment.
#  A build that links fine can still fail to load.  Wherever pgbson.so (or
#  pgbson.dylib) gets installed, check with ldd (or otool -L) that it knows
#  about libbson (libbson-1.0.so.0, or libbson2.so.2 for 2.x) e.g.:
#        $ ldd /usr/pgsql-16/lib/pgbson.so
#            linux-vdso.so.1 (0x00007ffc78f22000)
# good! -->  libbson-1.0.so.0 => /lib64/libbson-1.0.so.0 (0x00007fc6cfb20000)
#            libc.so.6 => /lib64/libc.so.6 (0x00007fc6cf800000)
#            /lib64/ld-linux-x86-64.so.2 (0x00007fc6cfb71000)
#
ifeq ($(origin BSON_INCLUDES)$(origin BSON_SHLIB),undefinedundefined)
  # Not needed just to clean, so do not insist on libbson for that.
  ifneq ($(if $(MAKECMDGOALS),$(filter-out clean distclean,$(MAKECMDGOALS)),all),)
    BSON_PC := $(shell pkg-config --exists bson2 2>/dev/null && echo bson2 || echo libbson-1.0)
    ifneq ($(shell pkg-config --exists $(BSON_PC) 2>/dev/null && echo ok),ok)
      $(error pkg-config cannot find libbson (tried bson2, libbson-1.0).  Install it, set PKG_CONFIG_PATH, or set BSON_INCLUDES and BSON_SHLIB; see the top of the Makefile)
    endif
    BSON_INCLUDES := $(shell pkg-config --cflags $(BSON_PC))
    BSON_SHLIB    := $(shell pkg-config --libs $(BSON_PC))
    $(info pgbson: libbson $(shell pkg-config --modversion $(BSON_PC)) via pkg-config ($(BSON_PC)): $(BSON_INCLUDES) $(BSON_SHLIB))
  endif
else ifeq ($(origin BSON_INCLUDES),undefined)
  $(error BSON_SHLIB is set but BSON_INCLUDES is not; set both, or neither to use pkg-config)
else ifeq ($(origin BSON_SHLIB),undefined)
  $(error BSON_INCLUDES is set but BSON_SHLIB is not; set both, or neither to use pkg-config)
endif


LOCAL_CFLAGS  =

# From here down is part of pgxs framework and should run anywhere.
# The variable names are very specific; do not mess with them.
MODULE_big = pgbson
EXTENSION = pgbson          # the extension's name
DATA = pgbson--2.0.sql pgbson--2.1.sql pgbson--2.2.sql pgbson--2.0--2.1.sql pgbson--2.1--2.2.sql
OBJS = pgbson.o

# Include paths go in PG_CPPFLAGS, not PG_CFLAGS: PGXS passes only CPPFLAGS to
# clang when it builds LLVM bitcode, so with PG_CFLAGS that step cannot find
# bson.h on a postgres built --with-llvm.
PG_CPPFLAGS = $(BSON_INCLUDES)
PG_CFLAGS = $(LOCAL_CFLAGS)
SHLIB_LINK = $(BSON_SHLIB)

# for postgres build
PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
