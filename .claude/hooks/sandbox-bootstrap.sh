#!/usr/bin/env bash
# Sandbox toolchain bootstrap — idempotent, runs on SessionStart.
#
# Ensures the system-level tooling needed to work on this repo is
# present after a sandbox restart. Repo bind-mount handles persistence
# of project files (.venv-linux, node_modules); this script handles
# what lives outside the mount: apt packages, ~/.bun, /etc env hooks.
#
# Safe to re-run anytime — each step is conditional.

set -euo pipefail

ENV_FILE=/etc/sandbox-persistent.sh
INSTALLED=()

# --- build tools (engine native deps: ruptures, cryptography, ...) ---
if ! command -v cc >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential
    INSTALLED+=("build-essential")
fi

# --- uv (engine Python toolchain) ------------------------------------
if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh
    INSTALLED+=("uv")
fi

# --- bun (cockpit toolchain) -----------------------------------------
if [ ! -x "$HOME/.bun/bin/bun" ]; then
    curl -fsSL https://bun.com/install | bash
    INSTALLED+=("bun")
fi

# --- /etc/sandbox-persistent.sh: PATH + UV venv isolation ------------
#
# UV_PROJECT_ENVIRONMENT=.venv-linux keeps the bind-mounted repo from
# colliding with the host's macOS-built .venv (broken symlinks under
# Linux). Both .venv and .venv-linux are gitignored.
if [ ! -f "$ENV_FILE" ] || ! sudo grep -q "UV_PROJECT_ENVIRONMENT" "$ENV_FILE"; then
    sudo tee -a "$ENV_FILE" <<'EOF' >/dev/null

# uv: OS-isolated project venv (sandbox-bootstrap)
export UV_PROJECT_ENVIRONMENT=".venv-linux"
EOF
    INSTALLED+=("UV_PROJECT_ENVIRONMENT")
fi

if [ ! -f "$ENV_FILE" ] || ! sudo grep -q 'BUN_INSTALL' "$ENV_FILE"; then
    sudo tee -a "$ENV_FILE" <<'EOF' >/dev/null

# bun PATH (sandbox-bootstrap)
export BUN_INSTALL="$HOME/.bun"
[ -d "$BUN_INSTALL/bin" ] && export PATH="$BUN_INSTALL/bin:$PATH"
EOF
    INSTALLED+=("bun PATH")
fi

if [ "${#INSTALLED[@]}" -gt 0 ]; then
    printf '[sandbox-bootstrap] installed: %s\n' "${INSTALLED[*]}"
fi
