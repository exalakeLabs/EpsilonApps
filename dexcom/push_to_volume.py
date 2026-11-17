#!/usr/bin/env python3
"""Copy new JSON files from ./export to the Unity Catalog volume with `databricks fs cp`.

Auth is whatever the Databricks CLI is configured with (`databricks auth login` / a profile
in ~/.databrickscfg, or DATABRICKS_HOST + DATABRICKS_TOKEN in the environment). Nothing
secret is stored by this script. Files already copied are listed in .pushed.json and skipped.

  python3 push_to_volume.py                    # default CLI profile
  python3 push_to_volume.py --profile myprof
"""
import argparse, json, shutil, subprocess, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DEST = 'dbfs:/Volumes/exalabs/dexcom/uploads/'
PUSHED = ROOT / '.pushed.json'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--profile')
    args = ap.parse_args()
    if not shutil.which('databricks'):
        sys.exit('databricks CLI not found. Install: brew tap databricks/tap && brew install databricks')

    done = set(json.loads(PUSHED.read_text())) if PUSHED.exists() else set()
    files = sorted(p for p in (ROOT / 'export').glob('*.json') if p.name not in done)
    if not files:
        print('nothing new to push'); return
    for p in files:
        cmd = ['databricks', 'fs', 'cp', '--overwrite', str(p), DEST + p.name]
        if args.profile:
            cmd += ['--profile', args.profile]
        r = subprocess.run(cmd, capture_output=True, text=True)
        if r.returncode != 0:
            sys.exit(f'upload failed for {p.name}:\n{r.stderr.strip() or r.stdout.strip()}')
        done.add(p.name)
        PUSHED.write_text(json.dumps(sorted(done), indent=2))
        print('uploaded', DEST + p.name)


if __name__ == '__main__':
    main()
