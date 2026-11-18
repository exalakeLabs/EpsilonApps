import { useMemo, useState } from 'react';
import { Card, Check, EmbedStage, Field, PageHeader, PersonaSelect, ScenarioSelect } from '../components/common';
import { SigmaFrame } from '../sigma/SigmaFrame';
import { applyParams, DEFAULT_PARAMS, redact, type EmbedParams } from '../lib/embedParams';
import { useEmbedSession, usePortal } from '../lib/hooks';

export function Basics() {
  const { scenarios, personas } = usePortal();
  const [scenarioId, setScenarioId] = useState(scenarios[0]?.id ?? '');
  const [personaId, setPersonaId] = useState(personas[0]?.id ?? '');
  const [p, setP] = useState<EmbedParams>(DEFAULT_PARAMS);
  const [height, setHeight] = useState<number | null>(null);
  const { session, reload } = useEmbedSession(scenarioId, personaId);
  const set = <K extends keyof EmbedParams>(k: K, v: EmbedParams[K]) => setP((o) => ({ ...o, [k]: v }));

  const finalUrl = useMemo(
    () => (session.status === 'ready' ? applyParams(session.url, p) : ''),
    [session, p],
  );

  return (
    <>
      <PageHeader title="Layout & Theming">
        Embed URL parameters change chrome and behavior without touching the workbook. Toggle them and the iframe reloads with the new URL.
      </PageHeader>
      <div className="split">
        <aside className="side">
          <Card title="Source">
            <ScenarioSelect value={scenarioId} onChange={setScenarioId} />
            <PersonaSelect value={personaId} onChange={setPersonaId} />
            <button className="btn" onClick={reload}>Mint new token</button>
          </Card>
          <Card title="URL parameters">
            <Field label="Theme (Light, Dark or custom name)">
              <input value={p.theme} placeholder="Light" onChange={(e) => set('theme', e.target.value)} />
            </Field>
            <Field label="Menu position">
              <select value={p.menuPosition} onChange={(e) => set('menuPosition', e.target.value as EmbedParams['menuPosition'])}>
                <option value="default">Default</option><option value="top">Top</option>
                <option value="bottom">Bottom</option><option value="none">Hidden</option>
              </select>
            </Field>
            <Field label="Language (e.g. fr, de, ja)">
              <input value={p.lng} onChange={(e) => set('lng', e.target.value)} />
            </Field>
            <Check label="Hide folder navigation" checked={p.hideFolderNavigation} onChange={(v) => set('hideFolderNavigation', v)} />
            <Check label="Hide tooltips" checked={p.hideTooltip} onChange={(v) => set('hideTooltip', v)} />
            <Check label="Hide view selector" checked={p.hideViewSelector} onChange={(v) => set('hideViewSelector', v)} />
            <Check label="Responsive height (iframe grows to content)" checked={p.responsiveHeight} onChange={(v) => { set('responsiveHeight', v); setHeight(null); }} />
          </Card>
          {session.status === 'ready' && (
            <Card title="Resulting URL"><code className="wrap">{redact(finalUrl)}</code></Card>
          )}
        </aside>
        <main className="main">
          <EmbedStage session={session}>
            {() => (
              <SigmaFrame
                src={finalUrl}
                title="Sigma embed"
                height={p.responsiveHeight && height ? height : '100%'}
                onEvent={(e) => {
                  if (e.name === 'workbook:pageheight:onchange') setHeight((e.payload as { pageHeight: number }).pageHeight);
                }}
              />
            )}
          </EmbedStage>
        </main>
      </div>
    </>
  );
}
