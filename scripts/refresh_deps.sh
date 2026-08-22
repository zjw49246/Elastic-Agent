#!/usr/bin/env bash
# Refresh declared production dependencies. The experimental claude-pty
# integration is intentionally excluded: its audited upstream revision does
# not provide a distributable license, and production must never fetch it from
# a mutable Git branch.
set -euo pipefail
cd "$(dirname "$0")/.."

# systemd ExecStartPre 的环境里没有 HOME/PATH 完整值
export HOME="${HOME:-/root}"
export PATH="$HOME/.local/bin:/usr/local/bin:$PATH"

UV="${UV:-$HOME/.local/bin/uv}"
command -v "$UV" >/dev/null || UV=uv
"$UV" sync --inexact
echo "production dependencies synced; experimental claude-pty was not fetched"
