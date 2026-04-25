#!/usr/bin/env bash
# Generate .local.plist files with real secrets and load them as LaunchAgents.
# The .local.plist files are git-ignored; templates (with REPLACE_WITH_* placeholders)
# are committed to git.
#
# Usage:
#   bash scripts/install_launchagents.sh singapore japansea blacksea
#   bash scripts/install_launchagents.sh --all
#   bash scripts/install_launchagents.sh --unload singapore japansea blacksea

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AGENTS_DIR="$REPO_DIR/config/launchagents"

# ---------------------------------------------------------------------------
# Secrets resolution — never hardcoded; must come from environment or ~/.maridb/env
# ---------------------------------------------------------------------------
ENV_FILE="$HOME/.maridb/env"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

AISSTREAM_API_KEY="${AISSTREAM_API_KEY:-}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-}"

if [[ -z "$AISSTREAM_API_KEY" ]]; then
  echo "Error: AISSTREAM_API_KEY is not set. Add it to ~/.maridb/env or export it." >&2
  exit 1
fi

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
UNLOAD=0
ALL=0
REGIONS=()

for arg in "$@"; do
  case "$arg" in
    --unload) UNLOAD=1 ;;
    --all)    ALL=1 ;;
    *)        REGIONS+=("$arg") ;;
  esac
done

if [[ $ALL -eq 1 ]]; then
  REGIONS=(singapore japansea blacksea europe middleeast persiangulf gulfofguinea gulfofaden gulfofmexico hornofafrica)
fi

if [[ ${#REGIONS[@]} -eq 0 ]]; then
  echo "Usage: $0 [--unload] [--all] <region> [<region> ...]"
  echo "       $0 singapore japansea blacksea"
  exit 1
fi

# ---------------------------------------------------------------------------
# Load / unload helper
# ---------------------------------------------------------------------------
process_region() {
  local region="$1"
  local template="$AGENTS_DIR/io.maridb.aisstream.${region}.plist"
  local local_plist="$AGENTS_DIR/io.maridb.aisstream.${region}.local.plist"

  if [[ ! -f "$template" ]]; then
    echo "  [skip] no template for region: $region"
    return
  fi

  if [[ $UNLOAD -eq 1 ]]; then
    launchctl unload "$local_plist" 2>/dev/null && echo "  unloaded: io.maridb.aisstream.$region" || echo "  not loaded: $region"
    return
  fi

  # Generate .local.plist with real secrets and resolved paths
  UV_BIN="$(command -v uv || echo /opt/homebrew/bin/uv)"
  DATA_DIR="$HOME/.maridb"
  LOG_DIR="$HOME/.maridb"
  mkdir -p "$DATA_DIR/data/raw/ais" "$LOG_DIR"
  sed \
    -e "s|REPLACE_WITH_AISSTREAM_API_KEY|$AISSTREAM_API_KEY|g" \
    -e "s|REPLACE_WITH_AWS_ACCESS_KEY_ID|${AWS_ACCESS_KEY_ID:-}|g" \
    -e "s|REPLACE_WITH_AWS_SECRET_ACCESS_KEY|${AWS_SECRET_ACCESS_KEY:-}|g" \
    -e "s|REPLACE_WITH_PROJECT_DIR/data/raw/ais|$DATA_DIR/data/raw/ais|g" \
    -e "s|REPLACE_WITH_PROJECT_DIR|$REPO_DIR|g" \
    -e "s|REPLACE_WITH_UV_BIN|$UV_BIN|g" \
    -e "s|REPLACE_WITH_HOME|$HOME|g" \
    -e "s|REPLACE_WITH_LOG_DIR|$LOG_DIR|g" \
    "$template" > "$local_plist"

  mkdir -p "$HOME/.maridb"
  launchctl unload "$local_plist" 2>/dev/null || true
  launchctl load "$local_plist"
  echo "  loaded: io.maridb.aisstream.$region"
}

process_r2sync() {
  local template="$AGENTS_DIR/io.maridb.r2sync.plist"
  local local_plist="$AGENTS_DIR/io.maridb.r2sync.local.plist"

  if [[ ! -f "$template" ]]; then return; fi

  if [[ $UNLOAD -eq 1 ]]; then
    launchctl unload "$local_plist" 2>/dev/null && echo "  unloaded: io.maridb.r2sync" || true
    return
  fi

  if [[ -z "$AWS_ACCESS_KEY_ID" ]]; then
    echo "  [skip] r2sync — AWS_ACCESS_KEY_ID not set"
    return
  fi

  sed \
    -e "s|REPLACE_WITH_AWS_ACCESS_KEY_ID|$AWS_ACCESS_KEY_ID|g" \
    -e "s|REPLACE_WITH_AWS_SECRET_ACCESS_KEY|$AWS_SECRET_ACCESS_KEY|g" \
    -e "s|REPLACE_WITH_PROJECT_DIR|$HOME/.maridb|g" \
    "$template" > "$local_plist"

  launchctl unload "$local_plist" 2>/dev/null || true
  launchctl load "$local_plist"
  echo "  loaded: io.maridb.r2sync"
}

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
echo "maridb LaunchAgents — $([ $UNLOAD -eq 1 ] && echo unload || echo load)"
echo ""

for region in "${REGIONS[@]}"; do
  process_region "$region"
done

# Always handle r2sync when loading
if [[ $UNLOAD -eq 0 ]]; then
  process_r2sync
fi

echo ""
echo "Done."
