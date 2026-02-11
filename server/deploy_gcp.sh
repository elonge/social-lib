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

if [[ -n "${PROJECT_ID}" ]]; then
  gcloud run deploy "${SERVICE_NAME}" \
    --source "${SERVER_DIR}" \
    --region "${REGION}" \
    --allow-unauthenticated \
    --set-env-vars "GOOGLE_API_KEY=${GOOGLE_API_KEY}" \
    --project "${PROJECT_ID}"
else
  gcloud run deploy "${SERVICE_NAME}" \
    --source "${SERVER_DIR}" \
    --region "${REGION}" \
    --allow-unauthenticated \
    --set-env-vars "GOOGLE_API_KEY=${GOOGLE_API_KEY}"
fi
