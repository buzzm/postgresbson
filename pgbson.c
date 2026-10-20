// Copyright (c) 2022  Buzz Moschetti <buzz.moschetti@gmail.com>
// 
// Permission to use, copy, modify, and distribute this software and its documentation for any purpose, without fee, and without a written agreement is hereby granted,
// provided that the above copyright notice and this paragraph and the following two paragraphs appear in all copies.
// 
// IN NO EVENT SHALL THE AUTHOR BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, 
// ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF THE AUTHOR HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
// 
// THE AUTHOR SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE.
// THE SOFTWARE PROVIDED HEREUNDER IS ON AN "AS IS" BASIS, AND THE AUTHOR HAS NO OBLIGATIONS TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.

// For future ref: 
// https://stackoverflow.com/questions/59801552/how-to-return-a-jsonb-object-from-a-postgresql-c-extension-function

#ifdef LOG
#undef LOG
#endif

//#define  PGBSON_DEBUG

#ifdef PGBSON_DEBUG
#include <stdio.h>    // only for fprintf() debugging....
#endif


// The Postgres family of #includes
#include <postgres.h>  // always need this first, and the deps are indented:
#include "utils/builtins.h"  // text_to_cstring, extern numeric_in
#include "utils/jsonb.h"  // JsonbPair type, funcs

// includes to support BSON<->timestamp
#include <utils/timestamp.h>
#include <datatype/timestamp.h>
#include <common/int.h>   // pg_sub_s64_overflow, pg_mul_s64_overflow

// includes to support BSON<->numeric
#include <utils/numeric.h>

// includes to support UTF-8 -> server encoding conversion:
#include <mb/pg_wchar.h>

// includes to support BSON binary send/receive:
#include <lib/stringinfo.h>  // technically, #included by pgformat but OK
#include <libpq/pqformat.h>

#include <fmgr.h> // always need this



// Others:  
#include "bson.h"  // obviously...

// libbson 2.x removed bson_array_as_json(); the identical (legacy extended
// JSON) renderer lives on as bson_array_as_legacy_extended_json(), which
// 1.30+ also has.  Older 1.x only knows the old name.
#if BSON_CHECK_VERSION(1, 30, 0)
#define pgbson_array_as_json bson_array_as_legacy_extended_json
#else
#define pgbson_array_as_json bson_array_as_json
#endif




// Our namespace for macros is BSON_ , acknowledging PG_ as the base namespace
#define DatumGetBson(X) ((bytea *) PG_DETOAST_DATUM_PACKED(X))
#define BSON_GETARG_BSON(n)  DatumGetBson(PG_GETARG_DATUM(n))

//  uint8_t* is "same" as char[] so hush up the compiler
//  AND:  Don't forget; we must use VARDATA_ANY when using PG_DETOAST_DATUM_PACKED
#define BSON_VARDATA_ANY(X)  (uint8_t*)VARDATA_ANY(X)      

// errmsg() is printf-like:  never hand it a variable as the format.
#define BSON_STATIC_INIT(BPTR,AA)					\
      do {if(!bson_init_static(BPTR, BSON_VARDATA_ANY(AA), VARSIZE_ANY_EXHDR(AA))) { ereport(ERROR, (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("cannot init bson in %s", __func__))); } }  while(0)

      


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif




static text* mk_text(const char* cstr)
{
    int s_len = strlen(cstr);
    int tot_len = s_len + VARHDRSZ;

    text* t = (text *) palloc(tot_len);
    SET_VARSIZE(t, tot_len);

    memcpy((void *) VARDATA(t), (void *) cstr, s_len);

    return t;
}


// BSON strings are UTF-8, but postgres text must be in the server encoding.
// pg_any_to_server() converts when the database is not UTF-8 (ERROR if a
// character has no equivalent there) and, when it is, verifies the bytes so
// invalid UTF-8 from rows stored before write-side validation cannot become
// invalid text.  ASCII is untouched, so this is free for the common case.
static text* mk_text_from_utf8(const char* utf8)
{
    char* conv = pg_any_to_server(utf8, strlen(utf8), PG_UTF8);
    text* t = mk_text(conv);

    if(conv != utf8) {
	pfree(conv);
    }

    return t;
}


static char* mk_cstring(const char* cstr)
{
    int s_len = strlen(cstr);
    int tot_len = s_len + 1;  // just 1 extra NULL...

    char* p = (char*) palloc(tot_len);

    memcpy((void *) p, (void *) cstr, tot_len); // incl NULL!

    return p;
}


static bytea* mk_palloc_bytea(bson_t* b)
{
    // b->len seems to be the official way to get at BSON length.

    int tot_size = b->len + VARHDRSZ; // MUST add varlena hdr!
    
    bytea* aa = (bytea*) palloc(tot_size);
    SET_VARSIZE(aa, tot_size);

    /*
     * VARDATA is a pointer to the data region of the new struct.  The source
     * could be a short datum, so retrieve its data through VARDATA_ANY.
     */
    memcpy((void *) VARDATA_ANY(aa), /* destination */
           (void *) bson_get_data(b), // ooo! an actual function, not b->data
	   b->len); 

    return aa;
}



PG_FUNCTION_INFO_V1(pgbson_version);
Datum pgbson_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(mk_text("2.1"));
}

// This is: take EJSON in, give back bytea* of BSON for storage in DB
PG_FUNCTION_INFO_V1(bson_in);
Datum bson_in(PG_FUNCTION_ARGS)
{
// Inbound string is EJSON
    char* jsons = PG_GETARG_CSTRING(0);
    int slen = strlen(jsons);

    bson_error_t err; // on stack

#ifdef PGBSON_DEBUG
    (void) fprintf(stderr, "bson_in()\n");
#endif
    
    bson_t* b = bson_new_from_json((const uint8_t *)jsons, slen, &err);

    if(b != NULL) {
	// Must generate a palloc'd copy so postgres can track the
	// memory!  Important to destroy b afterwards:
	bytea* aa = mk_palloc_bytea(b);

	bson_destroy(b);

	PG_RETURN_BYTEA_P(aa);

    } else {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_JSON_TEXT), errmsg("%s", err.message))
	    // "%s", not errmsg(err.message):  the libbson parse error can
	    // echo fragments of the input, so a stray %n in a bad literal
	    // would otherwise be interpreted as a format directive.
	    );

	PG_RETURN_NULL();
    }

}

// This is: get BSON pointer from DB, emit EJSON out
// VERY important consumers of this are the CLI and the to-JSON casting
// subsystem.
PG_FUNCTION_INFO_V1(bson_out);
Datum bson_out(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);

    size_t blen;
    bson_t b; // on stack
    char* jsons;
    
    BSON_STATIC_INIT(&b, aa);

#ifdef PGBSON_DEBUG
    (void) fprintf(stderr, "bson_out()\n");    
#endif
    
    //char* jsons = bson_as_json(&b, &blen); 
    // Use relaxed because it makes date handling in SQL MUCH easier...
    jsons = bson_as_relaxed_extended_json(&b, &blen);

    PG_FREE_IF_COPY(aa,0); // no need for ptr into &b anymore; char* jsons is
			   // alloc'd and stands on its own...
    
    if(jsons != NULL) {
	char* pstring = mk_cstring(jsons);  // palloc
	bson_free(jsons); // per bson.h
	PG_RETURN_CSTRING(pstring);
    }
	
    PG_RETURN_NULL();
}


// This is: take BSON pointer from DB, emit BSON to client
PG_FUNCTION_INFO_V1(bson_send);
Datum bson_send(PG_FUNCTION_ARGS)
{
    bytea* arg = BSON_GETARG_BSON(0);

    uint8_t* data = BSON_VARDATA_ANY(arg);
    uint32 sz = VARSIZE_ANY_EXHDR(arg);

    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, (char *) data, sz);

    PG_FREE_IF_COPY(arg,0);
    
    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


// Shared by bson_recv and pgbson_validate:  check that len bytes at data
// are well formed BSON, then return a palloc'd copy.  Errors if not.
static bytea* _validate_and_copy(const uint8_t* data, int len)
{
    bson_t b; // on stack
    bson_error_t error; // on stack
    bson_validate_flags_t flags;

    if(len < 5) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("bytes too short to be BSON"))
	    );
    }

    if(!bson_init_static(&b, data, len)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("basic BSON init fails; not BSON?"))
	    );
    }

    /*  No need for special checks for mongodb...
    bson_validate_flags_t flags = BSON_VALIDATE_UTF8
	| BSON_VALIDATE_DOLLAR_KEYS
	| BSON_VALIDATE_DOT_KEYS
	| BSON_VALIDATE_UTF8_ALLOW_NULL
	| BSON_VALIDATE_EMPTY_KEYS;
    */

    // Strings and keys must be valid UTF-8.  Otherwise bson_out() (and so
    // the json casts and pg_dump) fail on the row, bson_array_as_json()
    // cannot render it, and bson_get_string() hands invalid text to postgres.
    // ALLOW_NULL because BSON strings may legally carry an embedded NUL; the
    // text accessors truncate there (see bson_get_string).
    flags = BSON_VALIDATE_UTF8 | BSON_VALIDATE_UTF8_ALLOW_NULL;
    if(!bson_validate_with_error(&b, flags, &error)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("invalid BSON"))
	    // error.message is boring, just "corrupt BSON"; no indication of
	    // where the corruption occurred.
	    );
    }

    // OK!
    return mk_palloc_bytea(&b);
}


// This is: get BSON pointer from outside, save BSON in DB
// Called for binary format input:  COPY ... (FORMAT binary), and drivers
// that send parameters in binary.  The arg is the StringInfo message
// buffer; the BSON is everything from its cursor to the end, and we must
// consume it (advance the cursor) or postgres complains.  These bytes come
// straight off the wire so they get the same validation as bytea::bson.
PG_FUNCTION_INFO_V1(bson_recv);
Datum bson_recv(PG_FUNCTION_ARGS)
{
    StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);
    int len = buf->len - buf->cursor;
    const char* data = pq_getmsgbytes(buf, len);

    PG_RETURN_BYTEA_P(_validate_and_copy((const uint8_t*) data, len));
}


// This is: get bytea pointer from outside, validate it is good
// BSON, then make a copy and return it.
// Very important for safe insertion of BSON as bytea i.e.
//   insert into (bson_column) values ('\x000000'::bytea)
// to avoid as much as possible storing malformed/corrupt BSON
// It is pgbson_validate() because bson_validate() already exists!
PG_FUNCTION_INFO_V1(pgbson_validate);
Datum pgbson_validate(PG_FUNCTION_ARGS)
{
    bytea* bbb;

#ifdef PGBSON_DEBUG
    (void) fprintf(stderr, "bson_validate()\n");
#endif
    
    bytea* aa = BSON_GETARG_BSON(0); // No validation here...

    bbb = _validate_and_copy(BSON_VARDATA_ANY(aa), VARSIZE_ANY_EXHDR(aa));

    PG_FREE_IF_COPY(aa,0);
    
    PG_RETURN_BYTEA_P(bbb);
}


/**** 
 operators
****/

// Logical comparison
PG_FUNCTION_INFO_V1(pgbson_compare);
Datum pgbson_compare(PG_FUNCTION_ARGS)
{
    bytea* first = BSON_GETARG_BSON(0);
    bytea* second = BSON_GETARG_BSON(1);

    bson_t b1; // on stack
    bson_t b2; // on stack
    int cmp;
    
    bson_init_static(&b1, BSON_VARDATA_ANY(first), VARSIZE_ANY_EXHDR(first));
    bson_init_static(&b2, BSON_VARDATA_ANY(second), VARSIZE_ANY_EXHDR(second));

    cmp = bson_compare(&b1, &b2);

    PG_FREE_IF_COPY(first,0);
    PG_FREE_IF_COPY(second,1);
    
    PG_RETURN_INT32(cmp);
}


//  binary equality
PG_FUNCTION_INFO_V1(bson_binary_equal);
Datum bson_binary_equal(PG_FUNCTION_ARGS)
{
    bytea* first = BSON_GETARG_BSON(0);
    bytea* second = BSON_GETARG_BSON(1);

    bson_t b1; // on stack
    bson_t b2; // on stack
    bool cmp;
    
    bson_init_static(&b1, BSON_VARDATA_ANY(first), VARSIZE_ANY_EXHDR(first));
    bson_init_static(&b2, BSON_VARDATA_ANY(second), VARSIZE_ANY_EXHDR(second));
    
    cmp = bson_equal(&b1, &b2);
 
    PG_FREE_IF_COPY(first,0);
    PG_FREE_IF_COPY(second,1);
    
    PG_RETURN_BOOL(cmp);
}

// The =, <>, <, <=, >, >= and <<>> operators bind straight to these.  They
// used to be LANGUAGE SQL wrappers that called pgbson_compare()/
// bson_binary_equal() by bare name, so they were resolved through the
// CALLER's search_path at run time: anyone who could create a function of that
// name in an earlier schema on a victim's search_path got it run with the
// victim's privileges.  A C function has no body to resolve.  Symbols are
// pgbson_* because libbson already exports bson_equal and friends.
PG_FUNCTION_INFO_V1(pgbson_eq);
Datum pgbson_eq(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) == 0);
}

PG_FUNCTION_INFO_V1(pgbson_ne);
Datum pgbson_ne(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) != 0);
}

PG_FUNCTION_INFO_V1(pgbson_lt);
Datum pgbson_lt(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) < 0);
}

PG_FUNCTION_INFO_V1(pgbson_le);
Datum pgbson_le(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) <= 0);
}

PG_FUNCTION_INFO_V1(pgbson_gt);
Datum pgbson_gt(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) > 0);
}

PG_FUNCTION_INFO_V1(pgbson_ge);
Datum pgbson_ge(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(DatumGetInt32(pgbson_compare(fcinfo)) >= 0);
}

PG_FUNCTION_INFO_V1(pgbson_binary_ne);
Datum pgbson_binary_ne(PG_FUNCTION_ARGS)
{
    PG_RETURN_BOOL(!DatumGetBool(bson_binary_equal(fcinfo)));
}

// hash index support
PG_FUNCTION_INFO_V1(bson_hash);
Datum bson_hash(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);

    uint8_t* data = BSON_VARDATA_ANY(aa);
    uint32 sz = VARSIZE_ANY_EXHDR(aa);

    // Unsigned:  signed overflow is undefined in C.  Wraps to the same
    // bits the old signed int produced, so existing hash indexes are valid.
    uint32 hash = 5381; // ?
    uint32 c;

    for(uint32 n = 0; n < sz; n++) {
	c = *data++;
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }

    PG_FREE_IF_COPY(aa,0);    
    
    PG_RETURN_UINT32(hash);
}


// For now, no fancy conversions.  If you want fancy, let the casting machinery
// do its thing.  The overall API design is to mimic the bson lib itself,
// where you pull specific types from the object and "that's that."  In
// general, such explicit calls and behaviors minimizes implicit funny business
// that can happen when the deep C code starts making assumptions about things,
// for example numeric precision.
// We further optimize/mem-safe the environment by taking a text* as it would
// be passed from postgres and converting it to a regular char* here because
// we then have to pfree().
static bool _get_bson_iter(bson_t* b, text* dotpath, bson_iter_t* target, bson_type_t tt)
{
    bool rc = false;
    bson_iter_t iter;

    if (!bson_iter_init (&iter, b)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("iter BSON bytes corrupted"))
	    );
    } else {
	char* c_dotpath = text_to_cstring(dotpath);
	// Careful:  target is *already* pointer...
	rc = bson_iter_find_descendant(&iter, c_dotpath, target);
	pfree(c_dotpath);
    }
    if(rc) {
	bson_type_t ft = bson_iter_type(target);
	if(ft != tt) {
	    rc = false;
	}
    }

    return rc;
}

//
//  The get_<type> family all follow the same logic:
//
//  Put bson_t on the stack and init from bytea.  This avoids 
//  allocs at the extension level, essentially "setting the bytea into place"
//  and making the get_ very high performance.
//
//  Call our _get_bson_iter.  All bson_iter things rely on the host bson_t
//  remaining in scope; furthermore, if the thing is found, the per-type
//  extractors e.g. bson_iter_utf8() and bson_iter_int32() return material
//  that should NOT be freed and must also be used while the host bson_t
//  is in scope.  This is fine -- because returned material has to be
//  palloc'd anyway and the calling machinery will free() the text* and
//  bytea* material after the call is complete.
//
//  In all cases, be sure to clean up expanded TOAST data!
//


PG_FUNCTION_INFO_V1(bson_get_string);  // text bson_get_string(bson, dotpath)
Datum bson_get_string(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target;    
    bson_t b; // on stack
    bool rc;
    text* txt = 0;
    
    BSON_STATIC_INIT(&b,aa);
    
    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_UTF8);

    if(rc) {
	uint32_t len;
	// bson_iter_utf8() returns a pointer that MUST NOT be
	// modified or freed; fine, because we have to palloc it into
	// text anyway....
	// len is deliberately unused.  BSON strings may legally carry an
	// embedded NUL but postgres text may not, so mk_text's strlen()
	// truncating at the first NUL is the intended behavior.
	txt = mk_text_from_utf8( bson_iter_utf8(&target, &len) );
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc == true) PG_RETURN_TEXT_P(txt); else PG_RETURN_NULL();
}


// BSON datetime is signed int64 millis since the unix epoch, UTC.
// Postgres Timestamp is int64 micros since 2000-01-01.  So it is just
// shift the epoch and scale; no struct tm, no gmtime(), and negative
// (pre-1970) values need no special handling.  Returns false if the
// result is outside the range postgres timestamp can represent (BSON's
// range is far wider); the caller makes that NULL, not an error.
#define BSON_UNIX_TO_PG_EPOCH_MS \
    ((int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * SECS_PER_DAY * 1000)

static bool _cvt_datetime_to_ts(int64_t millis_since_epoch, Timestamp* ts)
{
    int64 pg_millis;

    if(pg_sub_s64_overflow(millis_since_epoch, BSON_UNIX_TO_PG_EPOCH_MS, &pg_millis)
       || pg_mul_s64_overflow(pg_millis, 1000, ts)) {
	return false;
    }

    return IS_VALID_TIMESTAMP(*ts);
}


PG_FUNCTION_INFO_V1(bson_get_datetime);
Datum bson_get_datetime(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    Timestamp ts;
    bson_iter_t target;
    bool rc = false;
    
    BSON_STATIC_INIT(&b,aa);

    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_DATE_TIME);
    if(rc) {
	int64_t millis_since_epoch = bson_iter_date_time (&target);

	rc = _cvt_datetime_to_ts(millis_since_epoch, &ts);
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc) PG_RETURN_TIMESTAMP(ts); else PG_RETURN_NULL();
}



PG_FUNCTION_INFO_V1(bson_get_decimal128);
Datum bson_get_decimal128(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target;
    bool rc = false;
    Numeric nm;
    
    bson_t b; // on stack
    BSON_STATIC_INIT(&b,aa);

    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_DECIMAL128);
    if(rc) {
	bson_decimal128_t val;

	if(bson_iter_decimal128(&target, &val)) {

	    // Safest way to convert is through a string bridge.
	    // From bson.h: max length of decimal128 string: BSON_DECIMAL128_STRING 43
	    char strbuf[43]; // TBD: #include bson-decimal128.h   ?
	    bson_decimal128_to_string(&val, strbuf);
	    
	    // OMG.   Googled this out of nowhere:
	    // https://www.spinics.net/lists/pgsql/msg185320.html
	    // This is how a "regular" piece of code can call the PG wrapper
	    // stuff, I guess mocking up the call semantics.  numeric_in is
	    // The Official string-to-numeric encoder from the postgres lib.
	    nm = DatumGetNumeric(DirectFunctionCall3(numeric_in,
							     CStringGetDatum(strbuf), 0, -1));
	} else {
	    rc = false;
	}
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc) PG_RETURN_NUMERIC(nm); else PG_RETURN_NULL();
}


static bool _get_obj_or_arr(bson_t* parent, text* dotpath, bson_t* child)
{
    bson_iter_t iter;
    bson_iter_t target;

    char* c_dotpath;
    bool rc;
    
    if(!bson_iter_init (&iter, parent)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("BSON iter bytes corrupted in bson_get_bson"))
	    );
    }

    c_dotpath = text_to_cstring(dotpath);
    rc = bson_iter_find_descendant(&iter, c_dotpath, &target);
    pfree(c_dotpath); // dotpath no longer needed
    
    if(rc) {
	uint32_t subdoc_len;
	const uint8_t* subdoc_data = 0;

	bson_type_t ft = bson_iter_type(&target);
	switch(ft) {
	case BSON_TYPE_DOCUMENT:  {
	    bson_iter_document(&target, &subdoc_len, &subdoc_data);
	    break;
	}
	case BSON_TYPE_ARRAY:  {
	    bson_iter_array(&target, &subdoc_len, &subdoc_data);
	    break;
	}
	default: {
	    // A scalar has no "object representation"; BSON cannot be built
	    // from anything but a key:value object.  Leave subdoc_data 0 so
	    // the caller is told "no child" and returns NULL.
	}
	}
	if(subdoc_data != 0) {
	    bson_init_static(child, subdoc_data, subdoc_len);
	} else {
	    // Path was found but yielded no document/array (scalar, or a
	    // failed bson_iter_document/array).  child was NEVER initialized,
	    // so we must not report success: callers pass child straight to
	    // mk_palloc_bytea()/bson_array_as_json(), and an uninitialized
	    // stack bson_t there is a segfault.
	    rc = false;
	}
    }
    
    return rc;
}
    

PG_FUNCTION_INFO_V1(bson_get_bson);  // bson bson_get_bson(bson, dotpath)
Datum bson_get_bson(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);    
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_t b2; // on stack
    bool rc;
    
    BSON_STATIC_INIT(&b,aa);

    rc = _get_obj_or_arr(&b, dotpath, &b2);
    
    if(rc) {
	bytea* bbb = mk_palloc_bytea(&b2); // alloc a *new* bytea to hold b2
	PG_FREE_IF_COPY(aa,0); // free the original if necessary...
	PG_RETURN_BYTEA_P(bbb); // return newly alloced material
    }
    
    PG_FREE_IF_COPY(aa,0);
    PG_RETURN_NULL();
}


/*
 * bson_get_value() is the general purpose extractor: it works for EVERY BSON
 * type, scalar or not.  BSON cannot be built from a bare scalar (or a bare
 * array), so the value found at the dotpath is returned wrapped in a
 * single-field document under the key "_":
 *
 *     path holds  {"corn":"dog"}   ->  {"_": {"corn":"dog"}}
 *     path holds  "hello"          ->  {"_": "hello"}
 *     path holds  [2,3,5,7]        ->  {"_": [2,3,5,7]}
 *
 * The wrap is UNCONDITIONAL, which is the whole point: the caller always peels
 * exactly one layer, so whatever emerges is the true value.  Wrapping only
 * scalars would make {"_": 5} ambiguous -- it could mean the scalar 5, or a
 * document that genuinely is {"_": 5}.  Wrapping everything removes that.
 *
 * Wrapping an array also sidesteps the string-indexed {"0":..,"1":..} form
 * that bson_get_bson() is forced into: inside the wrapper the array is a
 * VALUE, not the top level document, so drivers decode it as a real array.
 *
 * "_" and not "$value" deliberately: bson_out()/bson_in() round trip through
 * extended JSON, where a leading $ on a top level key risks being read back as
 * an EJSON type wrapper by bson_new_from_json().
 *
 * Note there is no -> style operator for this.  Arrow operators have to
 * compose with themselves and the wrapper breaks that -- every hop in a chain
 * would need an interposed ->'_'.  This function is terminal by design: call
 * it once, at the end of navigation, and unwrap.
 *
 * bson_iter_value() covers every BSON type in one shot, so unlike
 * bson_as_text() there is no per-type switch here -- and thus no per-type
 * bug surface.
 */
PG_FUNCTION_INFO_V1(bson_get_value);  // bson bson_get_value(bson, dotpath)
Datum bson_get_value(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b;   // on stack
    bson_t out; // libbson allocated; must be bson_destroy'd
    bson_iter_t iter;
    bson_iter_t target;
    bytea* bbb;
    char* c_dotpath;
    bool rc;

    BSON_STATIC_INIT(&b,aa);

#ifdef PGBSON_DEBUG
    (void) fprintf(stderr, "bson_get_value()\n");
#endif

    if(!bson_iter_init(&iter, &b)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("BSON iter bytes corrupted in bson_get_value"))
	    );
    }

    c_dotpath = text_to_cstring(dotpath);
    rc = bson_iter_find_descendant(&iter, c_dotpath, &target);
    pfree(c_dotpath); // dotpath no longer needed

    if(!rc) { // missing path is NULL, same as every other accessor
	PG_FREE_IF_COPY(aa,0);
	PG_RETURN_NULL();
    }

    bson_init(&out);
    bson_append_value(&out, "_", 1, bson_iter_value(&target));

    bbb = mk_palloc_bytea(&out); // copy into palloc'd memory...
    bson_destroy(&out);          // ...then let libbson free its own

    PG_FREE_IF_COPY(aa,0);
    PG_RETURN_BYTEA_P(bbb);
}


PG_FUNCTION_INFO_V1(bson_get_jsonb_array);
Datum bson_get_jsonb_array(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);    
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_t b2; // on stack
    bool rc;
    
    BSON_STATIC_INIT(&b,aa);

    rc = _get_obj_or_arr(&b, dotpath, &b2);

    if(rc) {
	size_t blen;
	// This is what makes it all possible:
	char* jsons = pgbson_array_as_json(&b2, &blen);
	Jsonb *jsonb;

	// bson_array_as_json() returns NULL when it cannot render the value,
	// e.g. a string that is not valid UTF-8.  jsonb_in(NULL) would
	// dereference it and take the whole backend down.
	if(jsons == NULL) {
	    PG_FREE_IF_COPY(aa,0);
	    ereport(
		ERROR,
		(errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("cannot convert BSON array to JSON"))
		);
	}

	// Well ...  HELLO there!
	jsonb = DatumGetJsonbP(DirectFunctionCall1(jsonb_in, CStringGetDatum(jsons)));
	bson_free(jsons); // per bson.h
	
	PG_FREE_IF_COPY(aa,0);
	
	PG_RETURN_POINTER(jsonb);
    }
    
    PG_FREE_IF_COPY(aa,0);
    PG_RETURN_NULL();
}



PG_FUNCTION_INFO_V1(bson_get_double);  // double bson_get_double(bson, dotpath)
Datum bson_get_double(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target;
    bool rc = false;
    double dbl = 0;  // doesnt matter
    bson_t b; // on stack
    
    BSON_STATIC_INIT(&b,aa);

    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_DOUBLE);
    if(rc) {
	dbl = bson_iter_double(&target);
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc == true) PG_RETURN_FLOAT8(dbl); else PG_RETURN_NULL();    
}

PG_FUNCTION_INFO_V1(bson_get_int32);  // int32 bson_get_int32(bson, dotpath)
Datum bson_get_int32(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target;
    bool rc = false;
    int32_t val = 0; // doesnt matter
    
    bson_t b; // on stack
    BSON_STATIC_INIT(&b,aa);

    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_INT32);
    if(rc) {
	val = bson_iter_int32(&target);
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc == true) PG_RETURN_INT32(val); else PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(bson_get_boolean);  // long bson_get_int64(bson, dotpath)
Datum bson_get_boolean(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target; 
    bool rc = false;
    bool val = false; // doesnt matter
    
    bson_t b; // on stack
    BSON_STATIC_INIT(&b,aa);
    
    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_BOOL);
    if(rc) {
	val = bson_iter_bool(&target);
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc == true) PG_RETURN_BOOL(val); else PG_RETURN_NULL();    
}


PG_FUNCTION_INFO_V1(bson_get_int64);  // long bson_get_int64(bson, dotpath)
Datum bson_get_int64(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target; 
    bool rc = false;
    int64_t val = 0; // doesnt matter
    
    bson_t b; // on stack
    BSON_STATIC_INIT(&b,aa);
    
    rc = _get_bson_iter(&b, dotpath, &target, BSON_TYPE_INT64);
    if(rc) {
	val = bson_iter_int64(&target);
    }

    PG_FREE_IF_COPY(aa,0);

    if(rc == true) PG_RETURN_INT64(val); else PG_RETURN_NULL();    
}


PG_FUNCTION_INFO_V1(bson_get_binary);
Datum bson_get_binary(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t target;    
    bson_t b; // on stack
    BSON_STATIC_INIT(&b,aa);

    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_BINARY)) {
	bson_subtype_t subtype;
	uint32_t len;
	const uint8_t* data;
	int tot_size;
	bytea* aa2;
	
	bson_iter_binary (&target, &subtype, &len, &data);
	// What to do with subtype?  Is there a way to return subtype along
	// with the main payload data...?
	
	tot_size = len + VARHDRSZ; // MUST add varlena hdr!
    
	aa2 = (bytea*) palloc(tot_size);
	SET_VARSIZE(aa2, tot_size);

	memcpy((void *) VARDATA(aa2), (void *) data, len); // VARDATA_ANY...?
	
	PG_FREE_IF_COPY(aa,0);
	PG_RETURN_BYTEA_P(aa2);
    }

    PG_FREE_IF_COPY(aa,0);
    PG_RETURN_NULL();
}




// ISO8601 UTC, e.g. 2022-03-03T12:13:14.789Z, with the ".mmm" only when the
// millis are non-zero.  This used to call libbson's _bson_iso8601_date_format,
// but that is a private symbol: it is not exported by current libbson and the
// shlib then fails to load ("undefined symbol").  The public API has no
// equivalent, so do it here.  Pure integer math (Howard Hinnant's
// civil_from_days), so no gmtime(), no timezone, and pre-1970 and far-out
// years just work.  Longest possible result is about 34 bytes.
static void _fmt_iso8601(int64_t millis_since_epoch, char* buf, size_t bufsz)
{
    int64_t days = millis_since_epoch / 86400000;
    int64_t msofday = millis_since_epoch % 86400000;
    int64_t z, era, doe, yoe, y, doy, mp, d, m;
    int secs, ms;

    if(msofday < 0) {  // C division truncates toward zero; we want floor
	msofday += 86400000;
	days -= 1;
    }

    z = days + 719468;
    era = (z >= 0 ? z : z - 146096) / 146097;
    doe = z - era * 146097;
    yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = yoe + era * 400;
    doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    if(m <= 2) {
	y += 1;
    }

    secs = (int)(msofday / 1000);
    ms = (int)(msofday % 1000);

    if(ms != 0) {
	snprintf(buf, bufsz, "%04lld-%02d-%02dT%02d:%02d:%02d.%03dZ",
		 (long long) y, (int) m, (int) d,
		 secs / 3600, (secs / 60) % 60, secs % 60, ms);
    } else {
	snprintf(buf, bufsz, "%04lld-%02d-%02dT%02d:%02d:%02dZ",
		 (long long) y, (int) m, (int) d,
		 secs / 3600, (secs / 60) % 60, secs % 60);
    }
}

PG_FUNCTION_INFO_V1(bson_as_text);  // text bson_get(bson, dotpath)
Datum bson_as_text(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_iter_t iter;
    bson_iter_t target;
    const char* txt = 0;
    char valbuf[64]; // good for numbers and dates
    bool must_free = false;
		
    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA_ANY(aa), VARSIZE_ANY_EXHDR(aa));

    if (!bson_iter_init (&iter, &b)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("bat BSON bytes corrupted"))
	    );
    } else {
	char* c_dotpath = text_to_cstring(dotpath);    
	bool rc = bson_iter_find_descendant(&iter, c_dotpath, &target);
	pfree(c_dotpath); // dotpath no longer needed
    
	if(rc) {
	    bson_type_t ft = bson_iter_type(&target);
	    switch(ft) {
	    case BSON_TYPE_UTF8: {
		uint32_t len;		
		// NO NEED TO free().  Truncates at an embedded NUL, as
		// bson_get_string does; postgres text cannot hold one.
		txt = bson_iter_utf8(&target, &len);
		break;
	    }
	    case BSON_TYPE_DOUBLE: {
		// Do NOT sprintf("%lf") into valbuf.  "%lf" is *fixed*
		// notation so the length scales with the exponent; 1e308
		// renders as 316 chars and blows the 64 byte stack buffer.
		// float8out is bounded, gives the shortest round-trip form
		// (3.1415, not "3.141500"), handles inf/nan, and matches how
		// postgres renders every other float8.  Result is palloc'd,
		// so it is NOT must_free (that path calls bson_free).
		double v = bson_iter_double(&target);
		txt = DatumGetCString(DirectFunctionCall1(float8out,
							 Float8GetDatum(v)));
		break;
	    }
	    case BSON_TYPE_INT32: {
		int32_t v = bson_iter_int32(&target);
		sprintf(valbuf, "%d", v);
		txt = valbuf;
		break;		
	    }
	    case BSON_TYPE_INT64: {
		int64_t v = bson_iter_int64(&target);
		sprintf(valbuf, "%lld", v);
		txt = valbuf;
		break;		
	    }
	    case BSON_TYPE_DECIMAL128: {
		bson_decimal128_t val;
		if(bson_iter_decimal128(&target, &val)) {
		    bson_decimal128_to_string(&val, valbuf);
		    txt = valbuf;
		}
		break;		
	    }
	    case BSON_TYPE_DATE_TIME: {
		int64_t millis_since_epoch = bson_iter_date_time (&target);
		_fmt_iso8601(millis_since_epoch, valbuf, sizeof(valbuf));
		txt = valbuf;
		break;
	    }

	    case BSON_TYPE_BOOL: {
		// "true"/"false" to match what jsonb's ->> yields, not the
		// "t"/"f" of postgres' own boolean type.  Both cast back to
		// boolean happily.  String literal, so no must_free; mk_text
		// copies it below.
		txt = bson_iter_bool(&target) ? "true" : "false";
		break;
	    }

	    case BSON_TYPE_OID: {
		// The bare 24 char hex, NOT the {"$oid": ...} EJSON wrapper,
		// consistent with how the other scalars here render (decimal128
		// and date do not carry their $-wrappers either).
		// bson_oid_to_string wants char[25]; valbuf is well clear.
		bson_oid_to_string(bson_iter_oid(&target), valbuf);
		txt = valbuf;
		break;
	    }

	    case BSON_TYPE_DOCUMENT: 
	    case BSON_TYPE_ARRAY: {		
		uint32_t subdoc_len;
		const uint8_t* subdoc_data;
		bson_t b; // on stack
		size_t blen;

		if(ft == BSON_TYPE_DOCUMENT) {
		    bson_iter_document(&target, &subdoc_len, &subdoc_data);
		} else {
		    bson_iter_array(&target, &subdoc_len, &subdoc_data);
		}

		bson_init_static(&b, subdoc_data, subdoc_len);

		txt = bson_as_relaxed_extended_json(&b, &blen);
		must_free = true;
		break;
	    }

	    case BSON_TYPE_BINARY: {				
		bson_subtype_t subtype;
		uint32_t len;
		const uint8_t* data;
		char* tmpp;
		int idx;
		
		// What to do with subtype?  Dunno!
		bson_iter_binary (&target, &subtype, &len, &data);

		// Output is "\x54252031..."  So 2 bytes for "\x",
		// then 2 slots to hold hex rep for each byte, plus 1 for NULL:
		tmpp = (char*)bson_malloc (2 + (len*2) + 1);

		tmpp[0] = '\\';
		tmpp[1] = 'x';
		idx = 2;  // start at 2, not 0, to jump over \x already in tmpp!
		for(int n = 0; n < len; n++) {
		    // Love that pointer math....
		    sprintf(tmpp+idx, "%02x", (uint8_t)data[n]);
		    idx += 2;
		}
		tmpp[idx] = '\0';

		txt = tmpp; // because txt is const char*
		must_free = true;
		break;
	    }

	    default: {
		break; // ?
	    }		
	    }
	}
    }

    if(txt != 0) {
	text* t = mk_text_from_utf8(txt);
	if(must_free) {
	    bson_free((void*)txt);
	}
	PG_FREE_IF_COPY(aa,0);
	PG_RETURN_TEXT_P(t);
    }
    
    PG_FREE_IF_COPY(aa,0);
    PG_RETURN_NULL();
}

/*
Experimental.  Will pursue one day, but bson_get_jsonb_array is DEF
good enough for the moment!

PG_FUNCTION_INFO_V1(bson_get_array_jsonb);
Datum bson_get_array_jsonb(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);    
    text* dotpath = PG_GETARG_TEXT_PP(1);

    (void) fprintf(stderr, "bson_get_array_jsonb()\n");
    
    JsonbPair *pair = palloc(sizeof(JsonbPair));
    pair->key.type = jbvString;
    pair->key.val.string.len = 3;
    pair->key.val.string.val = "foo";

    pair->value.type = jbvNumeric;
    pair->value.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, (int64_t)100));
  
    JsonbValue *object = palloc(sizeof(JsonbValue));
    object->type = jbvObject;
    object->val.object.nPairs = 1;
    object->val.object.pairs = pair;


    JsonbValue *arr = palloc(sizeof(JsonbValue));
    object->type = jbvArray;
    object->val.array.nElems = 1;
    object->val.array.elems = object;    
    
    PG_RETURN_POINTER(JsonbValueToJsonb(object));
}
*/

/*
PG_FUNCTION_INFO_V1(bson_get_array);
Datum bson_get_array(PG_FUNCTION_ARGS) {

    // _get_bson_iter_array
    //  bson_iter_array(&target, &subdoc_len, &subdoc_data);
    for(int i = 0; i < 3; ) {
    }

  pair->value.type = jbvNumeric;
  pair->value.val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric, (int64_t)100));
  
  JsonbValue *object = palloc(sizeof(JsonbValue));
  object->type = jbvObject;
  object->val.object.nPairs = 1;
  object->val.object.pairs = pair;

  PG_RETURN_POINTER(JsonbValueToJsonb(object));
}
*/
