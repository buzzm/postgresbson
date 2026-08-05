# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A PostgreSQL extension (`pgbson`) implementing a `bson` data type backed by the MongoDB C driver's
`libbson`. Very little is tracked in git — the whole extension is one C file plus one SQL script:

| File | Role |
| --- | --- |
| `pgbson.c` | The entire C implementation (~1000 lines, no headers of its own) |
| `pgbson--2.1.sql` | Extension install script (current): type, casts, operators, opclasses, `CREATE FUNCTION` bindings |
| `pgbson--2.0.sql` | The previous install script, frozen. Kept shippable so `CREATE EXTENSION pgbson VERSION '2.0'` still works and the upgrade path stays testable |
| `pgbson--2.0--2.1.sql` | Upgrade delta: the one `CREATE FUNCTION bson_get_value` |
| `pgbson.control` | Extension metadata for `CREATE EXTENSION` |
| `Makefile` | PGXS build; top half is hand-edited local paths |
| `pgbson_test.py` | The whole test suite (python + `psycopg2` + `bson`) |
| `META.json` | PGXN distribution metadata |
| `t1.sql`, `README.md`, `LICENSE` | |

The working directory contains many **untracked** scratch files (`perf*.py`, `ff*.py`, `pt*.py`,
`pgbson.c.OK`, `Makefile.20240117`, `MyMake.mk`, `NOTES`, built `.o`/`.so`/`.dylib`). These are the
author's local experiments and stale backups — not source of truth, and not to be modified or
cleaned up unless asked. `MyMake.mk` is a local variant of `Makefile` with Homebrew paths filled in.

## Build, install, test

The only external dependency is libbson (`brew install mongo-c-driver`, or `libbson-devel` on RH).
**Before the first build you must edit `BSON_INCLUDES` and `BSON_SHLIB` at the top of `Makefile`** to
point at the local libbson headers and shared lib; the committed values (`$(HOME)/projects/bson/...`)
work on nobody else's machine. Everything below that line is stock PGXS.

```bash
make PGUSER=postgres              # -> pgbson.o, pgbson.so (or .dylib on OS X)
make PGUSER=postgres install      # copies .so + .sql + .control into the postgres tree
# then RECONNECT (see below) before the new code is picked up
```

The author builds with the local Homebrew variant instead: `make -f MyMake.mk PGUSER=postgres`
and `make -f MyMake.mk PGUSER=postgres install`.

**Reconnect, not restart.** Postgres is process-per-connection and a backend `dlopen()`s the
shlib on the first call into it, then holds that code for the life of the process — it never
`dlclose()`s. So a *new connection* gets the newly installed `.so`; no server restart is
needed, which is why `pgbson_test.py` picks up changes on its own. What does *not* reload is
an already-open session: a `psql` left running across a `make install` keeps executing the old
C code silently, as would a pooler handing back a recycled backend. Restarting the server is
just the sledgehammer that guarantees no backend survives.

Note the `drop extension` / `create extension` in the test suite's `init()` is *not* what
reloads the C code, though it is easy to believe it is. It does re-read `pgbson--2.1.sql` from
disk, so **SQL-level** changes — a new `CREATE FUNCTION`, a changed operator binding — really
do take effect from the drop/create alone. Only the C half is tied to process lifetime.

Verify the installed shlib can actually find libbson at runtime — `ldd`/`otool -L` on the installed
`pgbson.so` must resolve `libbson-1.0.so.0`. A build that links fine but can't resolve at runtime is
the most common failure mode. On RHEL 9 the PGXS `.bc` LLVM target may fail for lack of `clang`; the
`.bc` is not needed for the extension to work.

```bash
python3 pgbson_test.py            # runs the full suite
```

The suite needs `psycopg2` and `pymongo` (for the `bson` module only). It connects using the **DSN
hardcoded at `pgbson_test.py:14`** — edit it for your local server. `init()` drops and recreates the
`pgbson` extension and a `bsontest` table on every run, so it always exercises the freshly installed
`.so`.

### Running a single test

`main()` holds a list of dicts, each keyed by a single-character marker (`pgbson_test.py:660`):

- `'-'` — run it (normal state)
- `'S'` — solo. If **any** test is marked `S`, only `S` tests run and the rest are silently skipped.
- `'M'` — mute. Skipped but reported as `skip`.

So to iterate on one test, flip its `'-'` to `'S'`, and flip it back when adding it to the suite.
Failures are reported by a test returning a non-`None` message string (or raising); returning `None`
means pass.

## Architecture

### The type is raw BSON bytes in a varlena

There is no in-memory struct. A `bson` datum *is* the BSON byte sequence with a varlena header, so
`bson`→`bytea` is a free cast (`WITHOUT FUNCTION`) and client drivers get the exact bytes back.
`storage = extended` means large values TOAST.

Consequences that pervade the C code:

- `BSON_GETARG_BSON(n)` detoasts *packed*, so all access must use `VARDATA_ANY` / `VARSIZE_ANY_EXHDR`
  (wrapped as `BSON_VARDATA_ANY`), never plain `VARDATA`.
- Every function puts a `bson_t` **on the stack** and points it at the datum with `bson_init_static`
  (macro `BSON_STATIC_INIT`) — no copying, no allocation. Anything libbson returns from an iterator
  (`bson_iter_utf8`, etc.) is a pointer *into* that datum: valid only while the `bson_t` is in scope,
  and must never be freed.
- Anything returned to postgres must be `palloc`'d. Helpers `mk_text`, `mk_cstring`, and
  `mk_palloc_bytea` exist for exactly this. Anything libbson allocated (`bson_as_*_json`,
  `bson_new_from_json`) must be copied into palloc'd memory and then `bson_free`/`bson_destroy`'d.
- Finish with `PG_FREE_IF_COPY(aa, 0)` to release detoasted copies.

### Text representation is EJSON, and that's the whole JSON strategy

`bson_in` = `bson_new_from_json` (accepts EJSON, so `{"$date":...}` / `{"$numberDecimal":...}` in an
`INSERT` literal become real BSON types). `bson_out` = `bson_as_relaxed_extended_json` — *relaxed*
deliberately, because it makes dates readable in SQL.

Because both directions are string-based, the SQL script gets JSON interop nearly for free:
`CREATE CAST (bson AS json) WITH INOUT` and the jsonb/json reverse casts all just route through
`bson_out`/`bson_in`. Any change to the text format therefore changes JSON casting behavior too.

Note the `$`-typename wrappers are part of the *output text* only, never part of a dotpath. Extract
`{"amt": {"$numberDecimal": ...}}` with `bson_get_decimal128(col, 'path.amt')`, not
`'path.amt.$numberDecimal'`.

### Write-path validation

`bytea`→`bson` is an implicit cast through `pgbson_validate`, which length-checks, `bson_init_static`s,
and `bson_validate_with_error`s before copying. This is the guard against storing malformed junk via
`insert ... values ('\x00...'::bytea)`. The read path assumes stored bytes are already valid and does
no revalidation — keep it that way; that assumption is what makes reads fast.

### The two accessor families

- **Dotpath accessors** (`bson_get_string`, `bson_get_int32`, `bson_get_datetime`,
  `bson_get_decimal128`, `bson_get_binary`, `bson_get_boolean`, `bson_get_bson`,
  `bson_get_jsonb_array`, `bson_as_text`). All share the helper `_get_bson_iter(b, dotpath, target, expected_type)`
  — one `bson_iter_find_descendant` walk plus a strict type check. `_get_obj_or_arr` is the analogous
  helper for document/array results.
- **Arrow operators**, which are thin SQL bindings: `->` is `bson_get_bson`, `->>` is `bson_as_text`.

Design invariant: **a wrong-typed or missing field returns NULL, never an error.** `ereport(ERROR)` is
reserved for corrupt BSON. `bson_as_text` is the one place with a per-BSON-type `switch` rendering
values to text.

### Operators and index support

`pgbson_compare` (libbson `bson_compare`) backs `=`/`<>`/`<`/`<=`/`>`/`>=`, each a one-line SQL
wrapper in `pgbson--2.1.sql`, and is the btree opclass support function. `bson_binary_equal` backs the
distinct `==`/`<<>>` binary-identity operators and `bson_hash` backs the hash opclass. Both
`bson_hash_ops` and `bson_btree_ops` are DEFAULT for the type.

### Naming: the `pgbson_` prefix collision rule

libbson already exports `bson_compare`, `bson_validate`, and `bson_version`. Those three (and only
those) are named `pgbson_compare`, `pgbson_validate`, `pgbson_version` in this extension. Everything
else keeps the natural `bson_` prefix.

## Adding or changing a function

A new accessor touches three files, in this order:

1. `pgbson.c` — the `Datum fn(PG_FUNCTION_ARGS)` plus its `PG_FUNCTION_INFO_V1(fn)`.
2. `pgbson--2.1.sql` — `CREATE FUNCTION ... AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;`
   (that qualifier set is used uniformly and is what makes functional indexes work).
   **And the matching upgrade script**, or existing installs never get the function: `CREATE
   EXTENSION` runs the install script only at install time. Adding to `pgbson--2.1.sql` alone
   serves fresh installs and nobody else. Either append to `pgbson--2.0--2.1.sql` if 2.1 is
   unreleased, or start a new `pgbson--2.1--2.2.sql` and bump `default_version` if it is out.
3. `pgbson_test.py` — a `check1` entry or a new function in the `all_tests` list.

Then `make install`. A changed `.so` is picked up by any **new connection**, so running
`pgbson_test.py` is enough; but the `drop extension` / `create extension` in its `init()` is not
what does it, and an already-open `psql` will keep running the old C code. See "Reconnect, not
restart" above.

## C style constraints

- PGXS compiles with `-Wdeclaration-after-statement`. Declarations go at the top of the block —
  commit `e8375bf` was a dedicated pass to clean these up, so don't reintroduce them.
- `LOCAL_CFLAGS = -Wno-format-security` exists because `ereport(... errmsg(var))` is used with
  non-literal messages (notably `err.message` from libbson and the `BSON_STATIC_INIT` buffer).
- `#define PGBSON_DEBUG` near the top of `pgbson.c` enables the `fprintf(stderr, ...)` tracing that
  most functions already carry.
- There are two large commented-out experimental blocks at the end of `pgbson.c`
  (`bson_get_array_jsonb`, `bson_get_array`) — attempts to build `Jsonb` values directly rather than
  via `jsonb_in`. They are not compiled and have no SQL bindings.

## The three version numbers are independent — do not "fix" them

`pgbson.control` (`default_version`), `META.json` (`"version"`), and the string returned by
`pgbson_version()` in `pgbson.c` all version **three different things** that move at different
rates: the postgres binding (i.e. the SQL install script and what `CREATE EXTENSION` sees), the
distribution published to PGXN, and the C code itself. They are not expected to agree, and
reconciling them is not a cleanup task.

**`pgbson.control` and `pgbson_version()` both currently read `2.1`. That is a coincidence, not a
correspondence.** They arrived there independently and will drift apart again; do not treat their
agreement as an invariant to preserve, and do not "restore" it if one moves.

`pgbson.control`'s `default_version` is the main switch: it decides what a bare `CREATE EXTENSION
pgbson` installs. Bumping it is a four-file move — rename the install script to match the new
version, add a `pgbson--<old>--<new>.sql` upgrade delta, list **all** the scripts in `DATA` in
both `Makefile` and `MyMake.mk`, and point `META.json`'s `"file"` at the new install script.
Old install scripts stay shipped rather than deleted, so `CREATE EXTENSION pgbson VERSION '2.0'`
keeps working and `extension_upgrade_path()` in the test suite has something real to upgrade from.
