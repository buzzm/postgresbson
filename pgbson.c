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

#include <stdio.h>    // only for fprintf() debugging....

// The Postgres family of #includes
#include <postgres.h>  // always need this first, and the deps are indented:
#include "utils/builtins.h"  // text_to_cstring, extern numeric_in
#include "utils/jsonb.h"  // JsonbPair type, funcs

// includes to support BSON<->timestamp
#include <utils/timestamp.h>
#include <datatype/timestamp.h>

// includes to support BSON<->numeric
#include <utils/numeric.h>

// includes to support BSON binary send/receive:
#include <lib/stringinfo.h>  // technically, #included by pgformat but OK
#include <libpq/pqformat.h>

#include <fmgr.h> // always need this



// Others:  Just one...
#include "bson.h"  // obviously...


// Our namespace for macros is BSON_ , acknowledging PG_ as the base namespace
#define DatumGetBson(X) ((bytea *) PG_DETOAST_DATUM_PACKED(X))
#define BSON_GETARG_BSON(n)  DatumGetBson(PG_GETARG_DATUM(n))

//  uint8_t* is "same" as char[] so hush up the compiler
#define BSON_VARDATA(X)  (uint8_t*)VARDATA(X)



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
	    (errcode(ERRCODE_INVALID_JSON_TEXT), errmsg(err.message))
	    );

	PG_RETURN_NULL();
    }

}

// This is: get BSON pointer from DB, emit EJSON out
// VERY important consumers of this are the CLI and and casting
// subsystem.
PG_FUNCTION_INFO_V1(bson_out);
Datum bson_out(PG_FUNCTION_ARGS)
{
    bytea* arg = BSON_GETARG_BSON(0);

    uint8_t* data = BSON_VARDATA(arg);
    uint32 sz = VARSIZE_ANY_EXHDR(arg);
    
    bson_t b; // on stack
    bson_init_static(&b, data, sz);

    size_t blen;

    //char* jsons = bson_as_json(&b, &blen); // TBD palloc()
    // Use relaxed because it makes date handling in SQL MUCH easier...
    char* jsons = bson_as_relaxed_extended_json(&b, &blen);

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

    uint8_t* data = BSON_VARDATA(arg);
    uint32 sz = VARSIZE_ANY_EXHDR(arg);

    StringInfoData buf;

    pq_begintypsend(&buf);
    pq_sendbytes(&buf, (char *) data, sz);

    PG_RETURN_BYTEA_P(pq_endtypsend(&buf));
}


// This is: get BSON pointer from outside, save BSON in DB
PG_FUNCTION_INFO_V1(bson_recv);
Datum bson_recv(PG_FUNCTION_ARGS)
{   
    StringInfo	buf = (StringInfo) PG_GETARG_POINTER(0);

    bson_t b; // on stack
    bson_init_static(&b, (const uint8_t*) buf, buf->len);
    
    bytea* aa = mk_palloc_bytea(&b);
	
    PG_RETURN_BYTEA_P(aa);
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
    bson_init_static(&b1, BSON_VARDATA(first), VARSIZE_ANY_EXHDR(first));
    bson_t b2; // on stack
    bson_init_static(&b2, BSON_VARDATA(second), VARSIZE_ANY_EXHDR(second));

    int cmp = bson_compare(&b1, &b2);
    
    PG_RETURN_INT32(cmp);
}


//  binary equality
PG_FUNCTION_INFO_V1(bson_binary_equal);
Datum bson_binary_equal(PG_FUNCTION_ARGS)
{
    bytea* first = BSON_GETARG_BSON(0);
    bytea* second = BSON_GETARG_BSON(1);

    bson_t b1; // on stack
    bson_init_static(&b1, BSON_VARDATA(first), VARSIZE_ANY_EXHDR(first));
    bson_t b2; // on stack
    bson_init_static(&b2, BSON_VARDATA(second), VARSIZE_ANY_EXHDR(second));
    
    bool cmp = bson_equal(&b1, &b2);

    PG_RETURN_BOOL(cmp);
}

// hash index support
PG_FUNCTION_INFO_V1(bson_hash);
Datum bson_hash(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);

    uint8_t* data = BSON_VARDATA(aa);
    uint32 sz = VARSIZE_ANY_EXHDR(aa);

    int hash = 5381; // ?
    int c;

    for(uint32 n = 0; n < sz; n++) {
	c = *data++;
        hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    }
    
    PG_RETURN_INT32(hash);
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
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("BSON bytes corrupted"))
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

PG_FUNCTION_INFO_V1(bson_get_string);  // text bson_get_string(bson, dotpath)
Datum bson_get_string(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_UTF8)) {
	uint32_t len;
	// bson_iter_utf8() returns a pointer that MUST NOT be
	// modified or freed; fine, because we have to palloc it into
	// text anyway....
	const char* val = bson_iter_utf8(&target, &len);

	// Index machinery does not know about cstring so we have to
	// use the fancier text* varlena implementation:
	PG_RETURN_TEXT_P(mk_text(val));
    }

    PG_RETURN_NULL();
}


static Timestamp _cvt_datetime_to_ts(int64_t millis_since_epoch)
{
    Timestamp ts;
    struct pg_tm tt, *tm = &tt;
    fsec_t fsec;
    
    time_t t_unix = millis_since_epoch/1000; // get seconds..
    unsigned t_ms = millis_since_epoch%1000; // ... and the millis...

    struct tm unix_tm = *gmtime(&t_unix); // * in front of function...?

    tt.tm_mday = unix_tm.tm_mday;
    tt.tm_mon  = unix_tm.tm_mon + 1;  // POSIX is 0-11; must add 1 for postgres
    tt.tm_year = unix_tm.tm_year + 1900; // POSIX is 1900; postgres starts at 1
	
    tt.tm_hour = unix_tm.tm_hour;
    tt.tm_min  = unix_tm.tm_min;
    tt.tm_sec  = unix_tm.tm_sec;

    fsec = t_ms * 1000; // turn millis to micros

    tm2timestamp(tm, fsec, NULL, &ts);

    return ts;
}


PG_FUNCTION_INFO_V1(bson_get_datetime);
Datum bson_get_datetime(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    Timestamp ts;

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_DATE_TIME)) {
	int64_t millis_since_epoch = bson_iter_date_time (&target);

	ts = _cvt_datetime_to_ts(millis_since_epoch);
	
	PG_RETURN_TIMESTAMP(ts);
    }

    PG_RETURN_NULL();    
}



PG_FUNCTION_INFO_V1(bson_get_decimal128);
Datum bson_get_decimal128(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_DECIMAL128)) {
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
	    Numeric nm = DatumGetNumeric(DirectFunctionCall3(numeric_in,
							     CStringGetDatum(strbuf), 0, -1));

	    PG_RETURN_NUMERIC(nm);
	}
    }

    PG_RETURN_NULL();
}



PG_FUNCTION_INFO_V1(bson_get_bson);  // bson bson_get_bson(bson, dotpath)
Datum bson_get_bson(PG_FUNCTION_ARGS)
{
    //bytea* aa = PG_GETARG_BYTEA_P(0);
    bytea* aa = BSON_GETARG_BSON(0);    
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t iter;
    bson_iter_t target;

    char* c_dotpath = text_to_cstring(dotpath);    

    if(!bson_iter_init (&iter, &b)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("BSON bytes corrupted"))
	    );
    }
    
    bool rc = bson_iter_find_descendant(&iter, c_dotpath, &target);

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
	    // ?  TBD How to "better" handle "object representation" of
	    // noncomplex types
	}
	}
	if(subdoc_data != 0) {
	    bson_t b2; // on stack
	    bson_init_static(&b2, subdoc_data, subdoc_len);
    
	    bytea* aa = mk_palloc_bytea(&b2);
	    
	    PG_RETURN_BYTEA_P(aa);
	}
    }
    
    PG_RETURN_NULL();
}



PG_FUNCTION_INFO_V1(bson_get_double);  // double bson_get_double(bson, dotpath)
Datum bson_get_double(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_DOUBLE)) {
	double dbl = bson_iter_double(&target);
	PG_RETURN_FLOAT8(dbl);
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(bson_get_int32);  // int32 bson_get_int32(bson, dotpath)
Datum bson_get_int32(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_INT32)) {
	int32_t val = bson_iter_int32(&target);
	PG_RETURN_INT32(val);
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(bson_get_int64);  // long bson_get_int64(bson, dotpath)
Datum bson_get_int64(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_INT64)) {
	int64_t val = bson_iter_int64(&target);
	PG_RETURN_INT64(val);
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(bson_get_binary);
Datum bson_get_binary(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);

    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));    

    bson_iter_t target;
    if(_get_bson_iter(&b, dotpath, &target, BSON_TYPE_BINARY)) {
	bson_subtype_t subtype;
	uint32_t len;
	const uint8_t* data;
    
	bson_iter_binary (&target, &subtype, &len, &data);

	// What to do with subtype?
	
	int tot_size = len + VARHDRSZ; // MUST add varlena hdr!
    
	bytea* aa2 = (bytea*) palloc(tot_size);
	SET_VARSIZE(aa2, tot_size);

	memcpy((void *) VARDATA(aa2), (void *) data, len); // VARDATA_ANY...?
	
	PG_RETURN_BYTEA_P(aa2);
    }

    PG_RETURN_NULL();
}




extern void
_bson_iso8601_date_format (int64_t msec_since_epoch, bson_string_t *str);

PG_FUNCTION_INFO_V1(bson_as_text);  // text bson_get(bson, dotpath)
Datum bson_as_text(PG_FUNCTION_ARGS)
{
    bytea* aa = BSON_GETARG_BSON(0);
    text* dotpath = PG_GETARG_TEXT_PP(1);
    
    bson_t b; // on stack
    bson_init_static(&b, BSON_VARDATA(aa), VARSIZE_ANY_EXHDR(aa));

    bson_iter_t iter;
    bson_iter_t target;

    const char* txt = 0;
    char valbuf[64]; // good for numbers and dates
    bool must_free = false;
		
    if (!bson_iter_init (&iter, &b)) {
	ereport(
	    ERROR,
	    (errcode(ERRCODE_INVALID_BINARY_REPRESENTATION), errmsg("BSON bytes corrupted"))
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
		txt = bson_iter_utf8(&target, &len); // NO NEED TO free()
		break;
	    }
	    case BSON_TYPE_DOUBLE: {
		double v = bson_iter_double(&target);
		sprintf(valbuf, "%lf", v);
		txt = valbuf;
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
		bson_string_t* str = bson_string_new (NULL);
		_bson_iso8601_date_format(millis_since_epoch, str);
		bson_strncpy (valbuf, str->str, str->len);
		valbuf[str->len] = '\0';
		bson_string_free(str, true); // true means "free segment" ?
		txt = valbuf;
		break;		
	    }								

	    case BSON_TYPE_DOCUMENT: 
	    case BSON_TYPE_ARRAY: {		
		uint32_t subdoc_len;
		const uint8_t* subdoc_data;

		if(ft == BSON_TYPE_DOCUMENT) {
		    bson_iter_document(&target, &subdoc_len, &subdoc_data);
		} else {
		    bson_iter_array(&target, &subdoc_len, &subdoc_data);
		}

		bson_t b; // on stack
		bson_init_static(&b, subdoc_data, subdoc_len);

		size_t blen;
		txt = bson_as_relaxed_extended_json(&b, &blen);
		must_free = true;
		break;
	    }

	    case BSON_TYPE_BINARY: {				
		bson_subtype_t subtype;
		uint32_t len;
		const uint8_t* data;
    
		// What to do with subtype?  Dunno!
		bson_iter_binary (&target, &subtype, &len, &data);

		// Output is "\x54252031..."  So 2 bytes for "\x",
		// then 2 slots to hold hex rep for each byte, plus 1 for NULL:
		char* tmpp = (char*)bson_malloc (2 + (len*2) + 1);

		tmpp[0] = '\\';
		tmpp[1] = 'x';
		int idx = 2;
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
	text* t = mk_text(txt);
	if(must_free) {
	    bson_free((void*)txt);
	}
	PG_RETURN_TEXT_P(t);
    }
    
    PG_RETURN_NULL();
}

/*
Experimental

The idea here is bson_get_array directly returns a jsonb so you do NOT
have to "back up" the dotpath to prevent the _to_bson() machinery exposing
the array as {"0":val1, "1":val2, ...}.  Given path.to.vector exists, then:

Meh:     select (bson_get_bson(bdata, 'path.to')::jsonb)->'vector'->>0 

Better!  select bson_get_array(bson_column, 'path.to.vector')->>0

This also has the benefit of not forcing the to-jsonb conversion of *all*
the fields in path.to.  If path.to.really_big_thing was a peer field to
vector, then resources would be wasting in converting it only to ignore it in
the subsequent arrow operator to get 'vector'.

Note in both cases, the double arrow operator is used as a terminal
operator to yield a text type.  If you want to treat it as, say, an integer,
then you must cast it "manually":

Meh:     select ((bson_get_bson(bdata, 'path.to')::jsonb)->'vector'->>0)::int

Better!  select (bson_get_array(bson_column, 'path.to.vector')->>0)::int

PG_FUNCTION_INFO_V1(bson_get_array);
Datum bson_get_array(PG_FUNCTION_ARGS) {
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
