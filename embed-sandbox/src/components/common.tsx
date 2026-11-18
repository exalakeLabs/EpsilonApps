import { useState, type ReactNode } from 'react';
import { usePortal, type Session } from '../lib/hooks';
import type { SigmaEvent } from '../sigma/events';

export function Json({ value }: { value: unknown }) {
  return <pre className="json">{JSON.stringify(value, null, 2)}</pre>;
}

export function PageHeader({ title, children }: { title: string; children: ReactNode }) {
  return (
    <header className="page-header">
      <h1>{title}</h1>
      <p>{children}</p>
    </header>
  );
}

export function Card({ title, children, actions }: { title: string; children: ReactNode; actions?: ReactNode }) {
  return (
    <section className="card">
      <div className="card-head">
        <h3>{title}</h3>
        {actions}
      </div>
      {children}
    </section>
  );
}

export function Check({ label, checked, onChange }: { label: string; checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <label className="check">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} />
      {label}
    </label>
  );
}

export function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
    </label>
  );
}

export function ScenarioSelect({ value, onChange }: { value: string; onChange: (id: string) => void }) {
  const { scenarios } = usePortal();
  return (
    <Field label="Workbook scenario">
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {scenarios.map((s) => (
          <option key={s.id} value={s.id}>
            {s.title}{s.configured ? '' : ' (not configured)'}
          </option>
        ))}
      </select>
    </Field>
  );
}

export function PersonaSelect({ value, onChange, label = 'Signed in as' }: { value: string; onChange: (id: string) => void; label?: string }) {
  const { personas } = usePortal();
  return (
    <Field label={label}>
      <select value={value} onChange={(e) => onChange(e.target.value)}>
        {personas.map((p) => (
          <option key={p.id} value={p.id}>{p.label}</option>
        ))}
      </select>
    </Field>
  );
}

/** Renders the embed once a URL is available; otherwise shows loading or setup guidance. */
export function EmbedStage({ session, children }: { session: Session; children: (url: string) => ReactNode }) {
  if (session.status === 'loading') return <div className="stage-empty">Requesting signed embed URL…</div>;
  if (session.status === 'error') {
    return (
      <div className="stage-empty error">
        <strong>Embed unavailable</strong>
        <p>{session.error}</p>
        <p className="muted">
          Copy <code>portal.config.example.json</code> to <code>portal.config.json</code>, set your workbook URLs, and
          add credentials to <code>.env</code>. Or try the <a href="#/playground">Playground</a> with a public embed URL.
        </p>
      </div>
    );
  }
  return <div className="stage">{children(session.url)}</div>;
}

export function EventLog({
  events, onClear, paused, onPause,
}: { events: SigmaEvent[]; onClear: () => void; paused: boolean; onPause: (p: boolean) => void }) {
  const [open, setOpen] = useState<number | null>(null);
  return (
    <Card
      title={`Event log (${events.length})`}
      actions={
        <span className="row">
          <button className="btn small" onClick={() => onPause(!paused)}>{paused ? 'Resume' : 'Pause'}</button>
          <button className="btn small" onClick={onClear}>Clear</button>
        </span>
      }
    >
      <ul className="log">
        {events.length === 0 && <li className="muted">Interact with the embed to see events arrive.</li>}
        {events.map((e) => (
          <li key={e.id}>
            <button className="log-row" onClick={() => setOpen(open === e.id ? null : e.id)}>
              <time>{new Date(e.at).toLocaleTimeString()}</time>
              <code>{e.name}</code>
            </button>
            {open === e.id && <Json value={e.payload} />}
          </li>
        ))}
      </ul>
    </Card>
  );
}
