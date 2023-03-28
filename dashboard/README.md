### Update conda environment
```commandline
conda env update -f environment.yml --prune
pip list --format=freeze --exclude=GDAL --exclude=grpcio-status > requirements.txt
```

### Try locally
```bash
source ../.env
docker buildx build -f Dockerfile -t dashboard_local .
docker run -p 8081:8081 -e PORT=8081 dashboard_local
```



### Create REDIS instance and VPC
```commandline
source .env
gcloud redis instances create $REDIS_INSTANCE_ID --region $REGION --project $PROJECT_ID

gcloud redis instances describe $REDIS_INSTANCE_ID --region $REGION --project $PROJECT_ID
gcloud redis instances describe $REDIS_INSTANCE_ID --region $REGION --project $PROJECT_ID --format "value(authorizedNetwork)"

# COPY IP TO .env

gcloud compute networks vpc-access connectors \
  create $CONNECTOR_NAME \
  --network default \
  --region $REGION \
  --project $PROJECT_ID \
  --subnet auto

```

### Deploy
```bash
source ../.env
source .env
docker buildx build -f Dockerfile -t dashboard . --platform linux/amd64
docker tag dashboard eu.gcr.io/$PROJECT_ID/dashboard:latest
docker push eu.gcr.io/$PROJECT_ID/dashboard:latest

gcloud run deploy dashboard \
      --project=$PROJECT_ID \
      --image=eu.gcr.io/$PROJECT_ID/dashboard:latest \
      --platform=managed \
      --region=$REGION \
      --timeout=60 \
      --concurrency=80 \
      --cpu=1 \
      --memory=4G \
      --max-instances=10  \
      --allow-unauthenticated \
      --vpc-connector $CONNECTOR_NAME \
      --set-env-vars REDISHOST=$REDISHOST,REDISPORT=$REDISPORT
```


Using AppEngine:
```commandline
gcloud app deploy app_production.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
```
