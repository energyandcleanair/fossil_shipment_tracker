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
