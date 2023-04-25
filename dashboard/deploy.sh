#!/usr/bin/env bash
source ../.env
source .env
docker buildx build -f Dockerfile -t dashboard . --platform linux/amd64 --no-cache
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
