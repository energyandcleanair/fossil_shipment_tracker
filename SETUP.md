

# Deploy API
```bash
gcloud app deploy app_development.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
gcloud app deploy app_production.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
```

# Transfer DB (Prod to dev)
```bash
pg_dump $DB_URL_PRODUCTION -F custom > "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT dropdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT createdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -T template0 "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -d "development" "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT -W $PGPORT_PASSWORD -d "development" "bkp/production_dump.out"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT home/hubert.thieriot/anaconda3/pkgs/postgresql-13.3-h2510834_1/bin/pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT -W $PGPORT_PASSWORD -d "development" "bkp/production_dump.out"
``````
