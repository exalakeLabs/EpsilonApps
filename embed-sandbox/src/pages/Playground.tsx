import { useEffect, useRef, useState } from 'react';
import { Card, EventLog, Field, PageHeader } from '../components/common';
import { SigmaFrame } from '../sigma/SigmaFrame';
import { useEventLog } from '../lib/hooks';

const KEY = 'embed-sandbox:playground-url';

function load(): string {
  try { return localStorage.getItem(KEY) ?? ''; } catch { return ''; }
}

export function Playground() {
  const [input, setInput] = useState(load);
  const [url, setUrl] = useState(load);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  const log = useEventLog();

  useEffect(() => {
    try { localStorage.setItem(KEY, url); } catch { /* storage unavailable */ }
  }, [url]);

  const valid = /^https:\/\/[^/]*sigmacomputing\.(com|io)\//.test(input.trim());

  return (
    <>
      <PageHeader title="Playground">
        Paste a public or already-signed Sigma embed URL to inspect SDK events with zero setup. The URL stays in your browser; nothing is sent to the portal server.
      </PageHeader>
      <Card title="Embed URL">
        <Field label="https://app.sigmacomputing.com/…">
          <input value={input} onChange={(e) => setInput(e.target.value)} placeholder="Paste an embed URL" />
        </Field>
        <button className="btn primary" disabled={!valid} onClick={() => setUrl(input.trim())}>Load</button>
        {input && !valid && <p className="muted">Only https://*.sigmacomputing.com or .io URLs are accepted.</p>}
      </Card>
      <div className="stage">
        {url ? <SigmaFrame src={url} title="Playground embed" iframeRef={iframeRef} onEvent={log.push} />
             : <div className="stage-empty">Load a URL to begin.</div>}
      </div>
      <EventLog {...log} onPause={log.setPaused} onClear={log.clear} />
    </>
  );
}
