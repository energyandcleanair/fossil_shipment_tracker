# On Mac
`Incompatible library version: libpoppler.108.dylib requires version 14.0.0 or later, but libfontconfig.1.dylib provides version 13.0.0`
```conda install fontconfig=2.13.1```


# Backup portcalls
```
pg_dump --file "portcall_backup" -d $DB_URL_DEVELOPMENT --verbose --format=t --blobs --table "public.portcall"
```


# Transfer from development to production
```bash
pg_dump -t ship $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t port $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t berth $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t terminal $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t portcall $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t position $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t flowdepartureberth $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t flowarrivalberth $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t flow $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION
pg_dump -t trajectory $DB_URL_DEVELOPMENT | psql $DB_URL_PRODUCTION


```



