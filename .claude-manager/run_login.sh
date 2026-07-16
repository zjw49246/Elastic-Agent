#!/bin/bash
# Worker-local login runner: Xvfb + vendored auto_login (P3 code path).
set -u
EMAIL="$1"; TOKEN="$2"; CFG="$3"
export PATH="$HOME/.local/bin:$PATH"

pkill -f "Xvfb :99" 2>/dev/null
rm -f /tmp/.X99-lock
nohup Xvfb :99 -screen 0 1280x1024x24 >/tmp/xvfb.log 2>&1 &
# wait for the X socket to appear
for i in $(seq 1 15); do
  [ -S /tmp/.X11-unix/X99 ] && break
  sleep 1
done
export DISPLAY=:99

echo "=== running auto_login ==="
cd "$HOME"
python3 auto_login.py --email "$EMAIL" --token "$TOKEN" --config-dir "$CFG"
RC=$?
echo "=== auto_login exit=$RC ==="
echo "=== config dir contents ($CFG) ==="
ls -la "$CFG" 2>/dev/null || echo "(no config dir)"
if [ -f "$CFG/.credentials.json" ]; then
  echo "=== .credentials.json keys ==="
  python3 -c "import json;d=json.load(open('$CFG/.credentials.json'));print(list(d.keys()));o=d.get('claudeAiOauth',{});print('oauth keys:',list(o.keys()));print('has accessToken:', bool(o.get('accessToken')))" 2>&1
fi
exit $RC
