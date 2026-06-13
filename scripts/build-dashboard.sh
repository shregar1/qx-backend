#!/usr/bin/env bash
#
# Rebuild the embedded qx-dashboard SPA and bake it into the qx
# release binary in one shot. (The Cargo *crate* is still named
# `qxd` internally; the *binary* it produces is `qx`.)
#
# What this does:
#   1. cd into ../qx-dashboard and run `vite build`.
#   2. Empty `crates/broker/static/dashboard/` and copy the new
#      `dist/` over it (this is the directory that `include_dir!`
#      pins at compile time).
#   3. Force-touch `crates/broker/src/dashboard_assets.rs` so cargo
#      sees the file mtime move and rebuilds the broker crate even
#      when only the embedded bytes changed.
#   4. `cargo build -p qxd --release`.
#
# Usage:
#   scripts/build-dashboard.sh
#
# The script is idempotent and safe to re-run.

set -euo pipefail

# Resolve the repo root regardless of where the script is invoked from.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUEUEX_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DASHBOARD_DIR="$(cd "$QUEUEX_DIR/../qx-dashboard" && pwd)"
EMBED_DIR="$QUEUEX_DIR/crates/broker/static/dashboard"
EMBED_TRIGGER="$QUEUEX_DIR/crates/broker/src/dashboard_assets.rs"

echo "qx-dashboard → $DASHBOARD_DIR"
echo "qx-broker embed dir → $EMBED_DIR"
echo

# 1. Vite build.
(
  cd "$DASHBOARD_DIR"
  if [ -x ./node_modules/.bin/vite ]; then
    echo ">>> npm i (cached node_modules detected — running for safety)"
    npm install --prefer-offline --no-audit --no-fund >/dev/null
  else
    echo ">>> npm i (cold)"
    npm install --no-audit --no-fund
  fi
  echo ">>> vite build"
  ./node_modules/.bin/vite build
)

# 2. Sync dist/ → embed dir.
echo ">>> sync dist/ → static/dashboard/"
rm -rf "$EMBED_DIR"/*
cp -R "$DASHBOARD_DIR/dist/." "$EMBED_DIR/"

# 3. Bump dashboard_assets.rs mtime so cargo rebuilds the embed.
#    include_dir! is evaluated at compile time; without a source
#    file change, cargo skips the rebuild and the old bundle stays
#    baked in.
echo ">>> touch $EMBED_TRIGGER"
touch "$EMBED_TRIGGER"

# 4. Rebuild qxd.
echo ">>> cargo build -p qxd --release"
(
  cd "$QUEUEX_DIR"
  cargo build -p qxd --release
)

cat <<DONE

qx is now built with the freshly-baked dashboard. Run it with:

  $QUEUEX_DIR/target/release/qx --config <path-to-qx.toml>

If no [metrics] section is set, the dashboard binds to
http://127.0.0.1:9090/ by default.
DONE
