#!/usr/bin/env python3
"""Fetch new Dexcom EGV readings and write them as JSON files into ./export.

Uses credentials.json + tokens.json (created by app.py). Refreshes the access token
and saves the rotated refresh token back to tokens.json. Keeps the last fetched
reading time in .fetch_state.json so each run only pulls what is new.

  python3 dexcom_fetch.py                 # incremental (first run: --backfill-days, default 30)
  python3 dexcom_fetch.py --backfill-days 90
"""
import argparse, json, os, sys, urllib.parse, urllib.request, urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = os.environ.get('DEXCOM_BASE', 'https://sandbox-api.dexcom.com')
REDIRECT = 'http://localhost:8080/callback'
EXPORT = ROOT / 'export'
STATE = ROOT / '.fetch_state.json'
OVERLAP = timedelta(hours=6)  # re-read a little to catch late-arriving readings; ingest dedupes on recordId


def write_private(path, obj):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(obj, f, indent=2)


def http(url, data=None, headers=None):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=60) as r:
            return json.load(r)
    except urllib.error.HTTPError as e:
        sys.exit(f'HTTP {e.code} from {url.split("?")[0]}: {e.read().decode()[:400]}')


def access_token():
    cfg = json.loads((ROOT / 'credentials.json').read_text())
    tokens = json.loads((ROOT / 'tokens.json').read_text())
    body = urllib.parse.urlencode(dict(
        client_id=cfg['client_id'], client_secret=cfg['client_secret'],
        refresh_token=tokens['refresh_token'], grant_type='refresh_token', redirect_uri=REDIRECT)).encode()
    new = http(BASE + '/v3/oauth2/token', body, {'Content-Type': 'application/x-www-form-urlencoded'})
    tokens.update(new)  # keeps the old refresh_token if Dexcom does not rotate it
    write_private(ROOT / 'tokens.json', tokens)
    return new['access_token']


def parse(s):
    return datetime.fromisoformat(s.replace('Z', '+00:00')).astimezone(timezone.utc).replace(tzinfo=None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--backfill-days', type=int, default=30)
    args = ap.parse_args()

    auth = {'Authorization': 'Bearer ' + access_token()}
    rng = http(BASE + '/v3/users/self/dataRange', headers=auth)
    end = parse(rng['egvs']['end']['systemTime']) + timedelta(seconds=1)
    if STATE.exists():
        start = parse(json.loads(STATE.read_text())['last_system_time']) - OVERLAP
    else:
        start = end - timedelta(days=args.backfill_days)

    fmt = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S')
    records, user_id, cursor = [], None, start
    while cursor < end:  # Dexcom allows at most 30 days per request
        chunk_end = min(cursor + timedelta(days=30), end)
        q = urllib.parse.urlencode(dict(startDate=fmt(cursor), endDate=fmt(chunk_end)))
        resp = http(BASE + '/v3/users/self/egvs?' + q, headers=auth)
        records += resp.get('records', resp.get('egvs', []))
        user_id = resp.get('userId', user_id)
        cursor = chunk_end

    if not records:
        print('no new readings'); return
    EXPORT.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = EXPORT / (f'egv_{fmt(start)}_{fmt(end)}_{stamp}.json'.replace(':', ''))
    write_private(out, {'recordType': 'egv', 'userId': user_id, 'fetchedAt': stamp, 'records': records})
    write_private(STATE, {'last_system_time': max(r['systemTime'] for r in records)})
    print(f'wrote {len(records)} readings -> {out.relative_to(ROOT)}')


if __name__ == '__main__':
    main()
