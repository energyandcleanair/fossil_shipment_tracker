


# Transfer DB (Prod to dev)
```bash
pg_dump $DB_URL_PRODUCTION -F custom > "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT dropdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT createdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -T template0 "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -d "development" "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT -W $PGPORT_PASSWORD -d "development" "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT home/hubert.thieriot/anaconda3/pkgs/postgresql-13.3-h2510834_1/bin/pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT -W $PGPORT_PASSWORD -d "development" "bkp/production_dump.out"
``````

# DB parameters
```postgresql
set work_mem = '16MB'; --has to be done in GCP console
set temp_file_limit = '8GB'; --has to be done in GCP console
alter table portcall alter column ship_imo set statistics 100000;
```
