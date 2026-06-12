#!/usr/bin/env bash
# 把 claude-pty 依赖刷新到 Claude-Code-PTY 仓库 main 最新 commit（CCM refresh_pty.sh 同款机制）。
#
# 背景：git 依赖是安装时快照，`git pull` 本仓库不会更新它。任何部署/同步流程
# 必须跑本脚本（或把它挂到 systemd ExecStartPre），保证拉取本仓库后上游代码一致：
#   git pull → ./scripts/refresh_deps.sh → restart
#
# 行为：
# - editable/本地安装（开发环境指向本地 PTY 仓库）→ 跳过，天然最新
# - 已安装 commit == PTY 远端 main HEAD → 跳过
# - 否则 uv lock --upgrade-package + sync 到最新 commit（同时更新 uv.lock，
#   保证 worker 侧按 lock pin 安装的 claude-pty 一并对齐）
set -euo pipefail
cd "$(dirname "$0")/.."

UV="${UV:-$HOME/.local/bin/uv}"
command -v "$UV" >/dev/null || UV=uv
PY=".venv/bin/python3"
[ -x "$PY" ] || PY=python3

PTY_URL=$(grep -A2 '\[tool.uv.sources\]' pyproject.toml | grep -oE 'https://[^"]+Claude-Code-PTY[^"]*' | head -1)
[ -n "$PTY_URL" ] || { echo "pyproject.toml 里找不到 claude-pty 的 uv source"; exit 1; }

if "$PY" -c "import claude_pty, sys; sys.exit(1 if 'site-packages' in claude_pty.__file__ else 0)" 2>/dev/null; then
    echo "claude-pty 是 editable/本地安装，跳过刷新"
    exit 0
fi

installed=$("$PY" - <<'PYEOF' 2>/dev/null || echo ""
import json, importlib.metadata as m
try:
    raw = m.distribution("claude-pty").read_text("direct_url.json") or "{}"
    print(json.loads(raw).get("vcs_info", {}).get("commit_id", ""))
except Exception:
    print("")
PYEOF
)

# 私有仓库 https 无凭证时走 ssh
export GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0="url.git@github.com:.insteadOf" GIT_CONFIG_VALUE_0="https://github.com/"
latest=$(git ls-remote "$PTY_URL" refs/heads/main | cut -f1)
[ -n "$latest" ] || { echo "无法获取 PTY 远端 main HEAD，跳过"; exit 0; }

if [ "$installed" = "$latest" ]; then
    echo "claude-pty 已是最新（${latest:0:12}）"
    exit 0
fi

echo "claude-pty: ${installed:0:12} -> ${latest:0:12}，刷新 lock + 重装…"
"$UV" lock --upgrade-package claude-pty
"$UV" sync --inexact
"$PY" -c "import claude_pty; print('import OK:', claude_pty.__file__)"
