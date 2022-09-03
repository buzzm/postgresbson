
drop table bsontest;
drop extension pgbson CASCADE;

create extension pgbson;

create table bsontest (
  dotpath text,
  bdata BSON,
  bdata2 BSON,
  jbdata jsonb,
  jdata json,
  ts timestamp without time zone,
  amt numeric(14,2)
  );


insert into bsontest (dotpath,ts,bdata,amt) values (
       'd.recordId',
       '2022-05-05T12:13:14.789Z',
       '{"d":{
           "recordId":"R1",
           "baz":27,
           "bigint":{"$numberLong":"88888888888888888"},
           "dbl":3.1415,
	   "ts": {"$date":"2022-03-03T12:13:14.789Z"},
	   "amt": {"$numberDecimal":"77777809838.97"},
	   "payload": {
	      "fun":"scootering",
	      "val":13,
	      "vector":[21,17,19],
	      "image" : { "$binary" : { "base64" : "VGhpcyBpcyBhIHRlc3Q=", "subType" : "00" } } 
	   }
         }
       }',
       234534.45
);


-- Ooooo!
CREATE INDEX ON bsontest ( bson_get_string(bdata, 'd.recordId'));

-- mydb=# select (data::json->'d'->'ts'->>'$date')::timestamp from btest;
