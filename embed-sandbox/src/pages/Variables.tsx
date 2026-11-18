import { useEffect, useRef, useState } from 'react';
import { Card, EmbedStage, EventLog, PageHeader, ScenarioSelect, PersonaSelect } from '../components/common';
import { SigmaFrame } from '../sigma/SigmaFrame';
import { commands, type SigmaEvent } from '../sigma/events';
import { useEmbedSession, useEventLog, usePortal } from '../lib/hooks';

type Vars = Record<string, string>;

export function Variables() {
  const { scenarios, personas } = usePortal();
  const [scenarioId, setScenarioId] = useState(scenarios[0]?.id ?? '');
  const [personaId, setPersonaId] = useState(personas[0]?.id ?? '');
  const { session } = useEmbedSession(scenarioId, personaId);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  const log = useEventLog();
  const [vars, setVars] = useState<Vars>({});
  const [draft, setDraft] = useState<Vars>({});
  const [newKey, setNewKey] = useState('');

  useEffect(() => { setVars({}); setDraft({}); }, [scenarioId, personaId]);

  const onEvent = (e: SigmaEvent) => {
    log.push(e);
    const p = e.payload as { workbook?: { variables: Vars }; variables?: Vars };
    const next = p.workbook?.variables ?? p.variables;
    if (['workbook:loaded', 'workbook:variables:onchange', 'workbook:variables:current'].includes(e.name) && next) {
      setVars((v) => ({ ...v, ...next }));
      setDraft((d) => ({ ...d, ...next }));
    }
  };

  const cmd = commands(iframeRef);
  const keys = Object.keys(draft);

  return (
    <>
      <PageHeader title="Host-driven Controls">
        Workbook controls are addressable by their Control ID. Edit a value here and it is pushed into the embed with <code>workbookVariablesUpdate</code>; change a control inside Sigma and <code>workbook:variables:onchange</code> updates this panel.
      </PageHeader>
      <div className="split">
        <aside className="side">
          <Card title="Source">
            <ScenarioSelect value={scenarioId} onChange={setScenarioId} />
            <PersonaSelect value={personaId} onChange={setPersonaId} />
          </Card>
          <Card
            title="Control values"
            actions={<button className="btn small" onClick={() => commands(iframeRef).listVariables()}>Refresh</button>}
          >
            {keys.length === 0 && <p className="muted">No variables yet. Controls appear after the workbook loads; make sure each control has a Control ID.</p>}
            {keys.map((k) => (
              <label key={k} className="field">
                <span>{k}{draft[k] !== vars[k] && ' •'}</span>
                <input value={draft[k]} onChange={(e) => setDraft({ ...draft, [k]: e.target.value })} />
              </label>
            ))}
            <div className="row">
              <input placeholder="Add Control ID" value={newKey} onChange={(e) => setNewKey(e.target.value)} />
              <button className="btn small" disabled={!newKey} onClick={() => { setDraft({ ...draft, [newKey]: '' }); setNewKey(''); }}>Add</button>
            </div>
            <button className="btn primary" disabled={!keys.length} onClick={() => cmd.updateVariables(draft)}>Apply to embed</button>
          </Card>
        </aside>
        <main className="main">
          <EmbedStage session={session}>
            {(url) => <SigmaFrame src={url} title="Sigma embed" iframeRef={iframeRef} onEvent={onEvent} />}
          </EmbedStage>
          <EventLog {...log} onPause={log.setPaused} onClear={log.clear} />
        </main>
      </div>
    </>
  );
}
