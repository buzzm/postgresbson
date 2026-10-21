# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A PostgreSQL extension (`pgbson`) implementing a `bson` data type backed by the MongoDB C driver's
`libbson`. Very little is tracked in git — the whole extension is one C file plus one SQL script:

| File | Role |
| --- | --- |
| `pgbson.c` | The entire C implementation (~1000 lines, no headers of its own) |
| `pgbson--2.2.sql` | Extension install script (current): type, casts, operators, opclasses, `CREATE FUNCTION` bindings |
| `pgbson--2.1.sql`, `pgbson--2.0.sql` | Previous install scripts, frozen. Kept shippable so `CREATE EXTENSION pgbson VERSION '2.0'` / `'2.1'` still work and the upgrade path stays testable |
| `pgbson--2.0--2.1.sql` | Upgrade delta: the one `CREATE FUNCTION bson_get_value` |
| `pgbson--2.1--2.2.sql` | Upgrade delta: rebinds the comparison operator functions from `LANGUAGE SQL` to C |
| `pgbson.control` | Extension metadata for `CREATE EXTENSION` |
| `Makefile` | PGXS build; top half is hand-edited local paths |
| `pgbson_test.py` | The whole test suite (python + `psycopg2` + `bson`) |
| `META.json` | PGXN distribution metadata |
| `mkpgxn.sh` | Builds the PGXN zip from a release tag (see "Releasing to PGXN") |
| `t1.sql`, `README.md`, `LICENSE` | |

The working directory contains many **untracked** scratch files (`perf*.py`, `ff*.py`, `pt*.py`,
`pgbson.c.OK`, `Makefile.20240117`, `MyMake.mk`, `NOTES`, built `.o`/`.so`/`.dylib`). These are the
author's local experiments and stale backups — not source of truth, and not to be modified or
cleaned up unless asked. `MyMake.mk` is a local variant of `Makefile` with Homebrew paths filled in.

## Build, install, test

The only external dependency is libbson (`libbson-devel` on Fedora/RHEL, `brew install
mongo-c-driver@1` on OS X). 1.x and 2.x both work. The `Makefile` finds it with `pkg-config`,
preferring `bson2` (2.x) over `libbson-1.0`, and prints the version and flags it picked as the
first line of the build; check that line when a build behaves oddly. Two ways to steer it:

- `PKG_CONFIG_PATH=/opt/homebrew/opt/mongo-c-driver@1/lib/pkgconfig make` — needed for brew's
  keg-only `mongo-c-driver@1`, which `pkg-config` does not see by default (it finds 1.23.2).
- Set **both** `BSON_INCLUDES` and `BSON_SHLIB` (command line or environment) and `pkg-config` is
  not run at all. Setting only one is an error. The include dir is the one *above* `bson/`,
  because `pgbson.c` does `#include <bson/bson.h>` (the form that works for 1.x and 2.x).

The include flags go in `PG_CPPFLAGS`, not `PG_CFLAGS`: PGXS passes only `CPPFLAGS` to clang for
the LLVM bitcode (`.bc`) step, so on a postgres built `--with-llvm` they would otherwise be
missing there. Homebrew's postgres is built without LLVM, so to exercise that step locally use
`make with_llvm=yes CLANG=clang`. Everything below the libbson section is stock PGXS.

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
reloads the C code, though it is easy to believe it is. It does re-read `pgbson--2.2.sql` from
disk, so **SQL-level** changes — a new `CREATE FUNCTION`, a changed operator binding — really
do take effect from the drop/create alone. Only the C half is tied to process lifetime.

Verify the installed shlib can actually find libbson at runtime — `ldd`/`otool -L` on the installed
`pgbson.so` must resolve `libbson-1.0.so.0` (`libbson2.so.2` for 2.x). A build that links fine but can't resolve at runtime is
the most common failure mode. If the PGXS `.bc` LLVM target fails for lack of `clang` (seen on RHEL 9), build with
`make with_llvm=no`; the `.bc` is not needed for the extension to work.

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
`insert ... values ('\x00...'::bytea)`. The binary-protocol entry point, `bson_recv` (`COPY ... (FORMAT
binary)`, binary-mode drivers), runs the same checks; both go through `_validate_and_copy()`. psycopg2
sends text, so only the binary-COPY tests in `pgbson_test.py` exercise `bson_recv`. The read path assumes stored bytes are already valid and does
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

`pgbson_compare` (libbson `bson_compare`) is the btree opclass support function and backs
`=`/`<>`/`<`/`<=`/`>`/`>=` through one-line C wrappers (`pgbson_eq`, `pgbson_ne`, `pgbson_lt`,
`pgbson_le`, `pgbson_gt`, `pgbson_ge`) that call it and test the result. `bson_binary_equal` backs
`==` directly, and `pgbson_binary_ne` wraps it for `<<>>`. `bson_hash` backs the hash opclass. Both
`bson_hash_ops` and `bson_btree_ops` are DEFAULT for the type.

**Operator functions must be C, never `LANGUAGE SQL`.** Through 2.1 they were SQL wrappers
calling `pgbson_compare()` by bare name. An SQL body resolves names through the *caller's*
`search_path` at run time, so a same-named function planted in an earlier schema ran instead,
with the caller's privileges. The extension is `relocatable`, so the body can't be
schema-qualified either. 2.2 (PR #6) rebound all seven to C; don't reintroduce SQL wrappers.

### Naming: the `pgbson_` prefix collision rule

libbson already exports `bson_compare`, `bson_validate`, and `bson_version`, so those three are
named `pgbson_compare`, `pgbson_validate`, `pgbson_version` in this extension, in both C and SQL.

The operator functions are the other exception, and only on the C side: their SQL names stay
`bson_equal`, `bson_not_equal`, `bson_lt`, `bson_lte`, `bson_gt`, `bson_gte`,
`bson_binary_not_equal`, but the C symbols they bind to (`AS 'MODULE_PATHNAME', 'pgbson_eq'`)
are `pgbson_eq`, `pgbson_ne`, `pgbson_lt`, `pgbson_le`, `pgbson_gt`, `pgbson_ge`,
`pgbson_binary_ne`. libbson already exports a C `bson_equal`, so that one had to be renamed, and
the rest follow it for consistency.

Everything else keeps the natural `bson_` prefix, with the same name in C and SQL.

## Adding or changing a function

A new accessor touches three files, in this order:

1. `pgbson.c` — the `Datum fn(PG_FUNCTION_ARGS)` plus its `PG_FUNCTION_INFO_V1(fn)`.
2. `pgbson--2.2.sql` — `CREATE FUNCTION ... AS 'MODULE_PATHNAME' LANGUAGE C STRICT IMMUTABLE PARALLEL SAFE;`
   (that qualifier set is used uniformly and is what makes functional indexes work).
   **And the matching upgrade script**, or existing installs never get the function: `CREATE
   EXTENSION` runs the install script only at install time. Adding to `pgbson--2.2.sql` alone
   serves fresh installs and nobody else. Either append to `pgbson--2.1--2.2.sql` if 2.2 is
   unreleased, or start a new `pgbson--2.2--2.3.sql` and bump `default_version` if it is out.
3. `pgbson_test.py` — a `check1` entry or a new function in the `all_tests` list.

Then `make install`. A changed `.so` is picked up by any **new connection**, so running
`pgbson_test.py` is enough; but the `drop extension` / `create extension` in its `init()` is not
what does it, and an already-open `psql` will keep running the old C code. See "Reconnect, not
restart" above.

## C style constraints

- PGXS compiles with `-Wdeclaration-after-statement`. Declarations go at the top of the block —
  commit `e8375bf` was a dedicated pass to clean these up, so don't reintroduce them.
- Never pass a variable as the `errmsg()` format: write `errmsg("%s", err.message)`. libbson's JSON
  parse errors can echo user input, so `errmsg(err.message)` is a format-string bug. The old
  `-Wno-format-security` suppression was removed in `d3112eb`; don't bring it back.
- `#define PGBSON_DEBUG` near the top of `pgbson.c` enables the `fprintf(stderr, ...)` tracing that
  most functions already carry.
- There are two large commented-out experimental blocks at the end of `pgbson.c`
  (`bson_get_array_jsonb`, `bson_get_array`) — attempts to build `Jsonb` values directly rather than
  via `jsonb_in`. They are not compiled and have no SQL bindings.

## The three version numbers are independent — do not "fix" them

Three *independent* things get versioned here, and they are not expected to agree:

| Where | Versions | Moves when |
| --- | --- | --- |
| `pgbson.control` `default_version` | the postgres binding — the SQL install script and what `CREATE EXTENSION` sees | the SQL surface changes |
| `META.json` top-level `"version"` | the distribution published to PGXN | you cut a PGXN release |
| `pgbson_version()` in `pgbson.c` | the C code itself | you feel like it |

Reconciling those three is not a cleanup task. As of this writing `default_version` reads
`2.2` and `pgbson_version()` reads `2.1` — they have already drifted apart, which is expected. Any
time they agree it is a coincidence, not a correspondence; do not treat agreement as an invariant,
and do not "restore" it when one moves.

**One genuine correspondence, though — do not let the rule above talk you out of it.**
`META.json`'s `provides.bson.version` is not the distribution version; per the PGXN meta spec it
is the version of *the extension*, so it is supposed to equal `pgbson.control`'s `default_version`.
Likewise `provides.bson.file` must name the current install script. Both must be updated when
`default_version` moves. (Unrelated but noted: the `provides` key and distribution `name` are both
`bson` while the extension is actually `pgbson` — a pre-existing mismatch, deliberately left alone.)

`pgbson.control`'s `default_version` is the main switch: it decides what a bare `CREATE EXTENSION
pgbson` installs. Bumping it is a four-file move — rename the install script to match the new
version, add a `pgbson--<old>--<new>.sql` upgrade delta, list **all** the scripts in `DATA` in
both `Makefile` and `MyMake.mk`, and update `META.json`'s `provides.bson.file` *and*
`provides.bson.version` per the correspondence above.
Old install scripts stay shipped rather than deleted, so `CREATE EXTENSION pgbson VERSION '2.0'`
keeps working and `extension_upgrade_path()` in the test suite has something real to upgrade from.

## Releasing to PGXN

A release is a tag, and the zip is built from the tag, never from the working directory. Most
pushes are not releases. To release X.Y.Z:

1. Set `META.json`'s top-level `"version"` to X.Y.Z and commit.
2. `git tag -a vX.Y.Z -m "PGXN X.Y.Z"`
3. `./mkpgxn.sh vX.Y.Z` — `git archive`s the tag into `bson-X.Y.Z.zip` (flat, explicit file list).
4. `git push origin main vX.Y.Z` — a plain `git push` does not send tags.
5. The user uploads the zip to PGXN by hand.

`mkpgxn.sh` refuses unless, at the tag, the tag name is `v` + `META.json` `"version"`,
`provides.bson.version` is `default_version` + `.0`, and `provides.bson.file` is
`pgbson--<default_version>.sql`. It also refuses to overwrite an existing zip: the
`bson-*.zip` files in the working directory are the uploaded originals.

A tag may be moved (`git tag -f`) until it is pushed. **Never move a pushed tag** — the PGDG RPM
packaging builds from tags. Fix forward with X.Y.Z+1.

Past releases map to these commits (verified by rebuilding each and diffing against the uploaded
zip): 2.0.2 `6730a5f`, 2.0.4 `ec71d31`, 2.1.0 `2eb7d20`, 2.1.1 `4547210`, 2.1.2 `a65d522`,
2.2.0 `30d1c66`. 2.1.1's commit is not on `main`: rebase-merging Devrim's PRs replaced it with
`01bc6e3`, which also contains his changes. The `v2.1.1` tag is what keeps `4547210` alive.
2.0.x predates the `provides` rule, so `mkpgxn.sh` refuses those tags; they never need rebuilding.
