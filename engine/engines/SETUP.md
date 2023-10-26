## Deploy API to AppEngine

```commandline
cd api
```

### Update conda environment
```commandline
conda env update -f environment.yml --prune
pip list --format=freeze --exclude=GDAL --exclude=grpcio-status > requirements.txt

# Replace pymongo with pymongo[srv] in requirements.txt
sed -i '' 's/pymongo==/pymongo[srv]==/g' requirements.txt
```

### Try locally


### Deploy
```bash
source .env
gcloud app deploy api_development.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
gcloud app deploy api_production.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
```
