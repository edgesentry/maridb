#!/usr/bin/env bash
# Install or unload the two indago LaunchAgents:
#   io.indago.aisstream  — AIS stream rotator (max 3 concurrent regions)
#   io.indago.r2sync     — hourly DuckDB → Parquet → R2 sync
#
# Usage:
#   bash scripts/install_launchagents.sh           # load both
#   bash scripts/install_launchagents.sh --unload  # unload both

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AGENTS_DIR="$REPO_DIR/config/launchagents"

# ---------------------------------------------------------------------------
# Secrets — from environment or ~/.indago/env
# ---------------------------------------------------------------------------
ENV_FILE="$HOME/.indago/env"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

AISSTREAM_API_KEY="${AISSTREAM_API_KEY:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"
S3_BUCKET="${S3_BUCKET:-indago-public}"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
UNLOAD=0
for arg in "$@"; do
  case "$arg" in
    --unload) UNLOAD=1 ;;
    *) echo "Unknown argument: $arg"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
UV_BIN="$(command -v uv || echo /opt/homebrew/bin/uv)"
DATA_DIR="$HOME/.indago"
LOG_DIR="$HOME/.indago/logs"

install_agent() {
  local name="$1"
  local template="$AGENTS_DIR/${name}.plist"
  local local_plist="$AGENTS_DIR/${name}.local.plist"

  if [[ $UNLOAD -eq 1 ]]; then
    launchctl unload "$local_plist" 2>/dev/null && echo "  unloaded: $name" || echo "  not loaded: $name"
    return
  fi

  mkdir -p "$DATA_DIR/data/raw/ais" "$DATA_DIR/data/staging/ais" "$LOG_DIR"

  sed \
    -e "s|REPLACE_WITH_AISSTREAM_API_KEY|$AISSTREAM_API_KEY|g" \
    -e "s|REPLACE_WITH_AWS_ACCESS_KEY_ID|${AWS_ACCESS_KEY_ID}|g" \
    -e "s|REPLACE_WITH_AWS_SECRET_ACCESS_KEY|${AWS_SECRET_ACCESS_KEY}|g" \
    -e "s|REPLACE_WITH_S3_BUCKET|${S3_BUCKET}|g" \
    -e "s|REPLACE_WITH_PROJECT_DIR|$REPO_DIR|g" \
    -e "s|REPLACE_WITH_UV_BIN|$UV_BIN|g" \
    -e "s|REPLACE_WITH_HOME|$HOME|g" \
    "$template" > "$local_plist"

  launchctl unload "$local_plist" 2>/dev/null || true
  launchctl load "$local_plist"
  echo "  loaded: $name"
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
echo "indago LaunchAgents — $([ $UNLOAD -eq 1 ] && echo unload || echo load)"
echo ""

if [[ $UNLOAD -eq 0 ]] && [[ -z "$AISSTREAM_API_KEY" ]]; then
  echo "Error: AISSTREAM_API_KEY is not set. Add it to ~/.indago/env or export it." >&2
  exit 1
fi

install_agent "io.indago.aisstream"

if [[ $UNLOAD -eq 0 ]] && [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
  echo "  [skip] io.indago.r2sync — AWS_ACCESS_KEY_ID not set"
else
  install_agent "io.indago.r2sync"
fi

echo ""
echo "Done."
