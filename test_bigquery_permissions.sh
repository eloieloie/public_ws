#!/bin/bash

# Test BigQuery permissions for the WIF service account
# Service Account: aks-denodo-updater-sa@tnn-sb-to970548-1.iam.gserviceaccount.com
# Project: tnn-sb-to970548-1

echo "Testing BigQuery permissions for WIF service account..."
echo "Service Account: aks-denodo-updater-sa@tnn-sb-to970548-1.iam.gserviceaccount.com"
echo "Project: tnn-sb-to970548-1"
echo "=================================================================="

SERVICE_ACCOUNT="aks-denodo-updater-sa@tnn-sb-to970548-1.iam.gserviceaccount.com"
PROJECT="tnn-sb-to970548-1"

echo ""
echo "1. Checking current IAM policy bindings for the service account:"
echo "----------------------------------------------------------------"
gcloud projects get-iam-policy $PROJECT \
    --flatten="bindings[].members" \
    --format="table(bindings.role)" \
    --filter="bindings.members:$SERVICE_ACCOUNT"

echo ""
echo "2. Testing specific BigQuery permissions:"
echo "----------------------------------------"

echo "Testing bigquery.jobs.create permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.jobs.create"

echo ""
echo "Testing bigquery.jobs.get permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.jobs.get"

echo ""
echo "Testing bigquery.datasets.get permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.datasets.get"

echo ""
echo "Testing bigquery.tables.list permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.tables.list"

echo ""
echo "Testing bigquery.tables.get permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.tables.get"

echo ""
echo "Testing bigquery.tables.getData permission:"
gcloud projects test-iam-permissions $PROJECT \
    --member="serviceAccount:$SERVICE_ACCOUNT" \
    --permissions="bigquery.tables.getData"

echo ""
echo "3. Checking if service account exists:"
echo "-------------------------------------"
gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT

echo ""
echo "4. Testing BigQuery access with the service account directly:"
echo "------------------------------------------------------------"
echo "Note: This requires impersonating the service account"
echo "Command to test (run separately if you have impersonation permissions):"
echo "gcloud auth activate-service-account $SERVICE_ACCOUNT --key-file=/path/to/key.json"
echo "bq ls --project_id=$PROJECT"

echo ""
echo "5. Alternative: Test BigQuery API access using current credentials:"
echo "-------------------------------------------------------------------"
echo "Testing if the project is accessible:"
gcloud projects describe $PROJECT

echo ""
echo "Testing BigQuery datasets in the project:"
bq ls --project_id=$PROJECT

echo ""
echo "6. Check Workload Identity Federation binding:"
echo "----------------------------------------------"
echo "Checking if KSA is properly bound to GSA..."
gcloud iam service-accounts get-iam-policy $SERVICE_ACCOUNT --project=$PROJECT

echo ""
echo "=================================================================="
echo "If permissions are missing, add them with:"
echo ""
echo "# Add BigQuery Job User role"
echo "gcloud projects add-iam-policy-binding $PROJECT \\"
echo "    --member=\"serviceAccount:$SERVICE_ACCOUNT\" \\"
echo "    --role=\"roles/bigquery.jobUser\""
echo ""
echo "# Add BigQuery Data Viewer role"
echo "gcloud projects add-iam-policy-binding $PROJECT \\"
echo "    --member=\"serviceAccount:$SERVICE_ACCOUNT\" \\"
echo "    --role=\"roles/bigquery.dataViewer\""
echo ""
echo "# Add BigQuery Metadata Viewer role"
echo "gcloud projects add-iam-policy-binding $PROJECT \\"
echo "    --member=\"serviceAccount:$SERVICE_ACCOUNT\" \\"
echo "    --role=\"roles/bigquery.metadataViewer\""
