### Update conda environment
```commandline
conda env update -f environment.yml --prune
pip list --format=freeze --exclude=GDAL --exclude=grpcio-status > requirements.txt

# Replace pymongo with pymongo[srv] in requirements.txt
sed -i '' 's/pymongo==/pymongo[srv]==/g' requirements.txt
```

### Try locally
```bash
source ../.env
docker buildx build -f Dockerfile -t dashboard_local .
docker run -p 8081:8081 -e PORT=8081 dashboard_local
```

### Deploy
```bash
source ../.env
docker buildx build -f Dockerfile -t dashboard . --platform linux/amd64
docker tag dashboard eu.gcr.io/$PROJECT_ID/dashboard:latest
docker push eu.gcr.io/$PROJECT_ID/dashboard:latest

gcloud run deploy dashboard \
      --project=$PROJECT_ID \
      --image=eu.gcr.io/$PROJECT_ID/dashboard:latest \
      --platform=managed \
      --region=europe-north1 \
      --timeout=60 \
      --concurrency=80 \
      --cpu=1 \
      --memory=1G \
      --max-instances=10  \
      --allow-unauthenticated
```
