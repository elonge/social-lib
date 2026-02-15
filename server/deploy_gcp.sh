#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
SERVER_DIR="${ROOT_DIR}/server"

SERVICE_NAME="${SERVICE_NAME:-book-extractor-api}"
REGION="${REGION:-us-central1}"
PROJECT_ID="${PROJECT_ID:-}"
ENV_FILE="${ENV_FILE:-}"

load_env_file() {
  local file="$1"
  if [[ -f "${file}" ]]; then
    echo "Loading env from ${file}"
    set -a
    # shellcheck disable=SC1090
    source "${file}"
    set +a
    return 0
  fi
  return 1
}

if [[ -n "${ENV_FILE}" ]]; then
  load_env_file "${ENV_FILE}" || {
    echo "ENV_FILE provided but not found: ${ENV_FILE}" >&2
    exit 1
  }
else
  load_env_file "${ROOT_DIR}/.env" || \
  load_env_file "${ROOT_DIR}/.env.local" || \
  load_env_file "${SERVER_DIR}/.env" || true
fi

if [[ -z "${GOOGLE_API_KEY:-}" ]]; then
  echo "GOOGLE_API_KEY is required." >&2
  echo "Example: GOOGLE_API_KEY=... ./server/deploy_gcp.sh" >&2
  echo "Or set ENV_FILE to a .env file that contains GOOGLE_API_KEY." >&2
  exit 1
fi

# Check for service-account.json
SERVICE_ACCOUNT_FILE="${SERVER_DIR}/service-account.json"
SERVICE_ACCOUNT_JSON=""

if [[ -f "${SERVICE_ACCOUNT_FILE}" ]]; then
  echo "Found service-account.json, injecting as environment variable..."
  # Minify JSON to single line
  SERVICE_ACCOUNT_JSON=$(tr -d '\n' < "${SERVICE_ACCOUNT_FILE}")
else
  echo "Warning: service-account.json not found in server directory."
  echo "Deployment may fail if the server cannot authenticate with Firebase."
fi

DEPLOY_CMD="gcloud run deploy ${SERVICE_NAME} \
  --source ${SERVER_DIR} \
  --region ${REGION} \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=${PROJECT_ID}"

if [[ -n "${PROJECT_ID}" ]]; then
  DEPLOY_CMD="${DEPLOY_CMD} --project ${PROJECT_ID}"
fi

if [[ -n "${SERVICE_ACCOUNT_JSON}" ]]; then
  # Escape quotes for the command line is tricky, but gcloud accepts comma-separated key=value
  # Since JSON contains commas, we should use a different separator or ensure it's quoted correctly.
  # Actually, gcloud set-env-vars handles usage if passed individually.
  # But simpler: Let's just append it.
  
  # Note: extremely long env vars can be an issue, but standard service accounts are small (~2KB).
  # We use --set-env-vars 'KEY=VALUE' syntax.
  
  # We need to escape single quotes if they exist in JSON (usually they don't, JSON uses double quotes)
  DEPLOY_CMD="${DEPLOY_CMD} --set-env-vars FIREBASE_SERVICE_ACCOUNT_JSON='${SERVICE_ACCOUNT_JSON}'"
fi

echo "Deploying to Cloud Run..."
eval "${DEPLOY_CMD}"
