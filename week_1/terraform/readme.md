### Development in terraform folder:

1. create gcp account
2. create service account (SA)
3. generate an access key json file for the SA. Download it to local dev environment
4. `export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"`
5. `gcloud auth application-default login`

### Minimum Required SA permissions for deployment
1. Storage Admin
2. Storage Object Admin

### Minimum Required API for terraform deployment to GCP
1. IAM Service Account Credentials API
2. Identity and Access Management (IAM) API
3. Cloud Storage
