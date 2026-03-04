import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from engine.version import APP_TITLE
from engine.version import APP_VERSION

ERRORS = []

def check_file_contains(path: Path, needle: str, label: str):
    if not path.exists():
        ERRORS.append(f"{label}: file not found: {path}")
        return
    txt = path.read_text(encoding='utf-8', errors='ignore')
    if needle not in txt:
        ERRORS.append(f"{label}: expected to contain '{needle}', but not found")

def check_config(path: Path, label: str):
    if not path.exists():
        ERRORS.append(f"{label}: file not found: {path}")
        return
    data = json.loads(path.read_text(encoding='utf-8', errors='ignore') or '{}')
    v = str(data.get('version') or '').strip()
    if v != APP_TITLE:
        ERRORS.append(f"{label}: version mismatch. config={v!r} expected={APP_TITLE!r}")

    app = data.get('app') if isinstance(data.get('app'), dict) else {}
    app_name = str(app.get('name') or '').strip()
    app_version = str(app.get('version') or '').strip()
    top_app_version = str(data.get('app_version') or '').strip()

    if app_name != APP_TITLE:
        ERRORS.append(f"{label}: app.name mismatch. app.name={app_name!r} expected={APP_TITLE!r}")
    if app_version != APP_VERSION:
        ERRORS.append(f"{label}: app.version mismatch. app.version={app_version!r} expected={APP_VERSION!r}")
    if top_app_version != APP_VERSION:
        ERRORS.append(f"{label}: app_version mismatch. app_version={top_app_version!r} expected={APP_VERSION!r}")

# 1) config.json
check_config(ROOT/'data'/'config.json', 'data/config.json')

# 2) README header
check_file_contains(ROOT/'README.md', APP_TITLE, 'README.md')

# 3) app/main.py must import version constants
check_file_contains(ROOT/'app'/'main.py', 'from engine.version import', 'app/main.py')

if ERRORS:
    print('VERSION CONSISTENCY: FAIL')
    for e in ERRORS:
        print(' -', e)
    sys.exit(1)
print('VERSION CONSISTENCY: OK ->', APP_TITLE)
