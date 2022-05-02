```bash
gcloud app deploy app_development.yaml --promote --stop-previous-version --project=fossil-shipment-tracker
gcloud app deploy app_production.yaml --promote --stop-previous-version --project=fossil-shipment-tracker

```