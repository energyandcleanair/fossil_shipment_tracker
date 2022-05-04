
## Transfer DB
```bash

pg_dump $DB_URL_DEVELOPMENT -F custom > "development_dump.out"

# Recreate production db
PGPASSWORD=$PGPASSWORD_PRODUCTION dropdb -h $PGHOST_PRODUCTION -U $PGUSER_PRODUCTION -p $PGPORT_PRODUCTION --no-password "production"
PGPASSWORD=$PGPASSWORD_PRODUCTION createdb -h $PGHOST_PRODUCTION -U $PGUSER_PRODUCTION -p $PGPORT_PRODUCTION --no-password -T template0 "production"
PGPASSWORD=$PGPASSWORD_PRODUCTION pg_restore -h $PGHOST_PRODUCTION -U $PGUSER_PRODUCTION -p $PGPORT_PRODUCTION --no-password -d "production" "development_dump.out"

```

```bash
pg_dump $DB_URL_PRODUCTION -F custom > "bkp/production_dump.out"

# Recreate development DB
PGPASSWORD=$PGPASSWORD_DEVELOPMENT dropdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT createdb -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -T template0 "development"
PGPASSWORD=$PGPASSWORD_DEVELOPMENT pg_restore -h $PGHOST_DEVELOPMENT -U $PGUSER_DEVELOPMENT -p $PGPORT_DEVELOPMENT --no-password -d "development" "bkp/production_dump.out"
``````

## Purpose
Create a data platform that provides decision-makers (political, business, financial), journalists and campaigning organizations with information that helps identify fossil fuel shipments from Russia, scandalize them and create momentum to stop purchases.

## Deliverables
- Daily tracking of the tonnage of fossil fuel shipments departing from Russian ports, by destination (reported by ship upon departure). The purpose is to track shipment volumes on a daily basis on the country level.
- Identification of completed voyages (ship loads in a Russian port, transits to a port outside of Russia, unloads) with information on berth locations, vessel type and tonnage
- Identification of the shore facilities (handling terminal, refinery, power plant…) and the ownership and financiers of those facilities; identification of vessel owner, insurer etc. commercial ties

## Methodology
See [methodology document](https://docs.google.com/document/d/19eB2Yk2mvx9fE1MXy1z5IYgRn11ag-OrzxCWUjxTMdo/edit?usp=sharing).

## How to contribute
Send us an [email](mailto:hubert@energyandcleanair.org). We'll add you to our Slack channel and Trello board.
