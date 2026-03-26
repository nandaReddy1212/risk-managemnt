# Install Chocolatey if you don't have it
Set-ExecutionPolicy Bypass -Scope Process -Force
[System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072
iex ((New-Object System.Net.WebClient).DownloadString('https://chocolatey.org/install.ps1'))

# Install Terraform
choco install terraform -y

# Verify
terraform version



### TO automate process from github to GCP through GIthub actions 
$env:PROJECT_ID = "Project_id"
$env:GITHUB_ORG="YOUR_GITHUB_USERNAME"
$env:REPO_NAME="riskplatform"

# Enable required APIs
gcloud services enable iamcredentials.googleapis.com
gcloud services enable sts.googleapis.com

# Create the Workload Identity Pool
gcloud iam workload-identity-pools create "github-pool" \
  --project=$PROJECT_ID \
  --location="global" \
  --display-name="GitHub Actions pool"

# Create the OIDC provider inside the pool
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
  --project=$PROJECT_ID \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --display-name="GitHub provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# Create a dedicated Terraform service account
gcloud iam service-accounts create terraform-sa \
  --project=$PROJECT_ID \
  --display-name="Terraform GitHub Actions SA"

# Grant it Owner role (fine for dev; tighten for prod)
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:terraform-sa@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/owner"

# Allow GitHub Actions to impersonate this SA
gcloud iam service-accounts add-iam-policy-binding \
  terraform-sa@${PROJECT_ID}.iam.gserviceaccount.com \
  --project=$PROJECT_ID \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')/locations/global/workloadIdentityPools/github-pool/attribute.repository/${GITHUB_ORG}/${REPO_NAME}"


## Get the Workload Identity Provider resource name to workflow key

gcloud iam workload-identity-pools providers describe github-provider \
  --project=$PROJECT_ID \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --format="value(name)"


## Step 4 — Add GitHub repository secrets
Go to your GitHub repo → Settings → Secrets and variables → Actions → New repository secret. Add these two:
Secret nameValueWIF_PROVIDERthe full provider resource name from Step3 
WIF_SERVICE_ACCOUNT = terraform account 


## Step 5 — Create the GitHub Actions workflow
In your repo create this file at .github/workflows/terraform.yml
and past terraform code to manage services from github
and create gcp gcs state bucket to manage terraform state file in remote backend


### if any error due to project number then recreate everystep after delete of github workflow actions