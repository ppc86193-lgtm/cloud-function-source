#!/usr/bin/env bash
set -euo pipefail
: "${BQLOC:=us-central1}"
env -u PYTHONPATH -u PYTHONHOME -u VIRTUAL_ENV CLOUDSDK_PYTHON_SITEPACKAGES=1 \
  bq --location="${BQLOC}" "$@"
