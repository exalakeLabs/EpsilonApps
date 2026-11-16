import json, os, secrets, html, time, threading
from pathlib import Path
from datetime import datetime, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlsplit, parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parent
CONFIG = json.loads((ROOT / 'credentials.json').read_text())
BASE = 'https://sandbox-api.dexcom.com'
REDIRECT = 'http://localhost:8080/callback'
states = {}
state_lock = threading.Lock()
status = {'connected': False, 'environment': 'sandbox'}

def save(name, value):
    path = ROOT / name
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f: json.dump(value, f, indent=2)

def api(path, token):
    req = Request(BASE + path, headers={'Authorization': 'Bearer ' + token})
    with urlopen(req, timeout=30) as r: return json.load(r)

class Handler(BaseHTTPRequestHandler):
    timeout = 15
    def log_message(self, *args): pass
    def respond(self, body, code=200, kind='text/html'):
        self.send_response(code)
        self.send_header('Content-Type', kind)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.end_headers()
        self.wfile.write(body.encode())
    def do_GET(self):
        if self.headers.get('Host') not in ('localhost:8080', '127.0.0.1:8080'):
            return self.respond('Invalid host', 400)
        url = urlsplit(self.path)
        if url.path == '/status':
            return self.respond(json.dumps(status), kind='application/json')
        if url.path == '/connect':
            state = secrets.token_urlsafe(32)
            with state_lock:
                for key, expires in list(states.items()):
                    if expires < time.time(): del states[key]
                states[state] = time.time() + 900
            query = urlencode(dict(client_id=CONFIG['client_id'], redirect_uri=REDIRECT, response_type='code', scope='offline_access', state=state))
            self.send_response(302)
            self.send_header('Location', BASE + '/v3/oauth2/login?' + query)
            self.end_headers()
            return
        if url.path == '/callback':
            q = parse_qs(url.query)
            state = q.get('state', [''])[0]
            with state_lock:
                expires = states.pop(state, 0)
            if not state or expires < time.time(): return self.respond('Invalid or expired OAuth state. Restart from <a href="/connect">Connect</a>.', 400)
            if 'error' in q: return self.respond('Dexcom authorization was declined.', 400)
            code = q.get('code', [''])[0]
            if not code: return self.respond('Missing authorization code', 400)
            stage = 'token_exchange'
            try:
                data = urlencode(dict(client_id=CONFIG['client_id'], client_secret=CONFIG['client_secret'], code=code, grant_type='authorization_code', redirect_uri=REDIRECT)).encode()
                with urlopen(Request(BASE + '/v3/oauth2/token', data=data, headers={'Content-Type': 'application/x-www-form-urlencoded'}), timeout=30) as r: tokens = json.load(r)
                save('tokens.json', tokens)
                stage = 'data_range'
                ranges = api('/v3/users/self/dataRange', tokens['access_token'])
                save('data-range.json', ranges)
                end = ranges['egvs']['end']['systemTime']
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00')) + timedelta(seconds=1)
                start_dt = end_dt - timedelta(days=1)
                fmt = lambda d: d.strftime('%Y-%m-%dT%H:%M:%S')
                stage = 'readings'
                readings = api('/v3/users/self/egvs?' + urlencode(dict(startDate=fmt(start_dt), endDate=fmt(end_dt))), tokens['access_token'])
                save('readings.json', readings)
                records = readings.get('records', readings.get('egvs', []))
                status.clear()
                status.update(connected=True, environment='sandbox', reading_count=len(records), start=fmt(start_dt), end=fmt(end_dt))
                save('status.json', status)
                self.send_response(303)
                self.send_header('Location', '/')
                self.send_header('Cache-Control', 'no-store')
                self.send_header('Referrer-Policy', 'no-referrer')
                self.end_headers()
                return
            except Exception as e:
                status.update(connected=False, stage=stage, error=type(e).__name__, http_status=getattr(e, 'code', None))
                save('status.json', status)
                return self.respond('Connection test failed: ' + html.escape(type(e).__name__) + '. See /status for status.', 502)
        if url.path == '/':
            if status.get('connected'):
                return self.respond('<h1>Dexcom sandbox connected</h1><p>Retrieved ' + str(status['reading_count']) + ' simulated glucose readings.</p><p>Saved locally in readings.json.</p>')
            return self.respond('<h1>Dexcom sandbox connection test</h1><p>Uses simulated data only.</p><a href="/connect">Connect Dexcom sandbox</a>')
        self.respond('Not found', 404)

if __name__ == '__main__':
    print('Dexcom sandbox callback listening at http://localhost:8080', flush=True)
    ThreadingHTTPServer(('127.0.0.1', 8080), Handler).serve_forever()
