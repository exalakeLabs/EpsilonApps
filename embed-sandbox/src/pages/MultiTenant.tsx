import { useState } from 'react';
import { Card, EmbedStage, Json, PageHeader, PersonaSelect, ScenarioSelect } from '../components/common';
import { SigmaFrame } from '../sigma/SigmaFrame';
import { useEmbedSession, usePortal } from '../lib/hooks';

function Pane({ scenarioId, initial, label }: { scenarioId: string; initial: string; label: string }) {
  const [personaId, setPersonaId] = useState(initial);
  const { session, reload } = useEmbedSession(scenarioId, personaId);
  return (
    <div className="pane">
      <Card title={label} actions={<button className="btn small" onClick={reload}>Re-sign</button>}>
        <PersonaSelect value={personaId} onChange={setPersonaId} />
        {session.status === 'ready' && session.claims && (
          <details>
            <summary>JWT claims sent to Sigma</summary>
            <Json value={session.claims} />
            <p className="muted">Expires {session.expiresAt ? new Date(session.expiresAt).toLocaleTimeString() : 'n/a'}</p>
          </details>
        )}
      </Card>
      <div className="stage-short">
        <EmbedStage session={session}>{(url) => <SigmaFrame src={url} title={label} />}</EmbedStage>
      </div>
    </div>
  );
}

export function MultiTenant() {
  const { scenarios, personas } = usePortal();
  const secure = scenarios.filter((s) => s.auth === 'secure');
  const [scenarioId, setScenarioId] = useState(secure[0]?.id ?? scenarios[0]?.id ?? '');

  return (
    <>
      <PageHeader title="Multi-tenant Security">
        With secure embedding, your server signs a JWT per viewer. Sigma uses its <code>user_attributes</code>, <code>teams</code> and <code>account_type</code> claims to apply row-level security and permissions, so the same workbook shows different data per tenant. Identity is resolved on the server; the browser only sends a persona id.
      </PageHeader>
      <Card title="Workbook"><ScenarioSelect value={scenarioId} onChange={setScenarioId} /></Card>
      <div className="compare">
        <Pane key={`a-${scenarioId}`} scenarioId={scenarioId} initial={personas[0]?.id ?? ''} label="Viewer A" />
        <Pane key={`b-${scenarioId}`} scenarioId={scenarioId} initial={personas[1]?.id ?? personas[0]?.id ?? ''} label="Viewer B" />
      </div>
      <Card title="How to wire up row-level security">
        <ol>
          <li>In Sigma, create a user attribute (for example <code>region</code>) under Administration.</li>
          <li>In your data model or workbook, filter with <code>CurrentUserAttributeText("region")</code>.</li>
          <li>Pass the value in <code>user_attributes</code> when signing, as <code>server/jwt.ts</code> does.</li>
        </ol>
      </Card>
    </>
  );
}
