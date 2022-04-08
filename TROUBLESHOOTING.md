# On Mac
`Incompatible library version: libpoppler.108.dylib requires version 14.0.0 or later, but libfontconfig.1.dylib provides version 13.0.0`
```conda install fontconfig=2.13.1```


`Library not loaded: /usr/local/opt/openldap/lib/libldap_r-2.4.2.dylib
  Referenced from: /usr/local/opt/osgeo-postgresql/lib/libpq.5.dylib`
```bash
 brew uninstall osgeo-postgresql
 brew uninstall postgresql
 brew install postgresql
 brew install postgis
```

# Backup portcalls
```
pg_dump --file "portcall_backup" -d $DB_URL_DEVELOPMENT --verbose --format=t --blobs --table "public.portcall"
```