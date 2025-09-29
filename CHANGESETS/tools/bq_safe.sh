#!/usr/bin/env bash
set -euo pipefail
PROJECT="${PROJECT:-wprojectl}"
BQLOC="${BQLOC:-us-central1}"
bq --project_id="${PROJECT}" --location="${BQLOC}" "$@"
