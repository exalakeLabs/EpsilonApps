import json, os, secrets, html, time, threading
from pathlib import Path
from datetime import datetime, timedelta
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlencode, urlsplit, parse_qs
from urllib.request import Request, urlopen
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parent
BASE = 'https://sandbox-api.dexcom.com'
REDIRECT = 'http://localhost:8080/callback'
states = {}
state_lock = threading.Lock()
status = {'connected': False, 'environment': 'sandbox'}

def load_credentials():
    config = json.loads((ROOT / 'credentials.json').read_text())
    if any(not isinstance(config.get(key), str) or not config[key].strip()
           for key in ('client_id', 'client_secret')):
        raise ValueError('Missing client credentials')
    return {key: config[key].strip() for key in ('client_id', 'client_secret')}

def error_details(error, stage):
    """Return actionable errors without echoing provider text, codes or secrets."""
    oauth_error = None
    if isinstance(error, HTTPError):
        try:
            payload = json.loads(error.read(8192))
            candidate = payload.get('error') if isinstance(payload, dict) else None
            if candidate in ('invalid_client', 'unauthorized_client', 'invalid_grant',
                             'invalid_request', 'invalid_scope', 'access_denied'):
                oauth_error = candidate
        except (ValueError, OSError):
            pass
        finally:
            error.close()
    if stage == 'token_exchange' and oauth_error in ('invalid_client', 'unauthorized_client'):
        message = ('Dexcom rejected the app credentials. Copy the current Client ID and '
                   'Client Secret from the Sigma app in Dexcom My Apps into credentials.json, '
                   'then start a new connection below.')
    elif stage == 'token_exchange' and oauth_error == 'invalid_grant':
        message = ('Dexcom rejected the authorization code. It may have expired or already '
                   'been used. Start a new connection below; do not reload the old callback URL.')
    else:
        message = ('The Dexcom connection failed during ' + stage.replace('_', ' ') +
                   '. Start a new connection below. See /status for the failure stage and HTTP status.')
    return dict(connected=False, environment='sandbox', stage=stage,
                error=type(error).__name__, http_status=getattr(error, 'code', None),
                oauth_error=oauth_error, message=message)

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
    def redirect(self, location, code=303):
        self.send_response(code)
        self.send_header('Location', location)
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.end_headers()
    def do_GET(self):
        if self.headers.get('Host') not in ('localhost:8080', '127.0.0.1:8080'):
            return self.respond('Invalid host', 400)
        url = urlsplit(self.path)
        if url.path == '/status':
            return self.respond(json.dumps(status), kind='application/json')
        if url.path == '/connect':
            try:
                config = load_credentials()
            except (OSError, ValueError):
                return self.respond('Check credentials.json contains a Client ID and Client Secret.', 500)
            state = secrets.token_urlsafe(32)
            with state_lock:
                for key, pending in list(states.items()):
                    if pending['expires'] < time.time(): del states[key]
                states[state] = dict(expires=time.time() + 900, credentials=config)
            query = urlencode(dict(client_id=config['client_id'], redirect_uri=REDIRECT, response_type='code', scope='offline_access', state=state))
            self.redirect(BASE + '/v3/oauth2/login?' + query, 302)
            return
        if url.path == '/callback':
            q = parse_qs(url.query)
            state = q.get('state', [''])[0]
            with state_lock:
                pending = states.pop(state, None)
            if not pending or pending['expires'] < time.time(): return self.respond('Invalid or expired OAuth state. Restart from <a href="/connect">Connect</a>.', 400)
            config = pending['credentials']
            if 'error' in q: return self.respond('Dexcom authorization was declined.', 400)
            code = q.get('code', [''])[0]
            if not code: return self.respond('Missing authorization code', 400)
            stage = 'token_exchange'
            try:
                data = urlencode(dict(client_id=config['client_id'], client_secret=config['client_secret'], code=code, grant_type='authorization_code', redirect_uri=REDIRECT)).encode()
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
                self.redirect('/')
                return
            except Exception as e:
                status.clear()
                status.update(error_details(e, stage))
                print('Dexcom error:', stage, status['http_status'], status['oauth_error'], flush=True)
                save('status.json', status)
                return self.redirect('/')
        if url.path == '/':
            if status.get('connected'):
                return self.respond('<h1>Dexcom sandbox connected</h1><p>Retrieved ' + str(status['reading_count']) + ' simulated glucose readings.</p><p>Saved locally in readings.json.</p>')
            if status.get('message'):
                return self.respond('<h1>Dexcom connection needs attention</h1><p>' + html.escape(status['message']) + '</p><a href="/connect">Start a new sandbox connection</a>')
            return self.respond('<h1>Dexcom sandbox connection test</h1><p>Uses simulated data only.</p><a href="/connect">Connect Dexcom sandbox</a>')
        self.respond('Not found', 404)

if __name__ == '__main__':
    print('Dexcom sandbox callback listening at http://localhost:8080', flush=True)
    ThreadingHTTPServer(('127.0.0.1', 8080), Handler).serve_forever()
