import { useRef, useState } from 'react';
import { Card, EmbedStage, EventLog, Field, Json, PageHeader, PersonaSelect, ScenarioSelect } from '../components/common';
import { SigmaFrame } from '../sigma/SigmaFrame';
import { commands, type SigmaEvent } from '../sigma/events';
import { useEmbedSession, useEventLog, usePortal } from '../lib/hooks';

const SELECTION_EVENTS = [
  'workbook:table:oncellselect',
  'workbook:pivottable:oncellselect',
  'workbook:chart:onvalueselect',
];

export function Interactions() {
  const { scenarios, personas } = usePortal();
  const [scenarioId, setScenarioId] = useState(scenarios[0]?.id ?? '');
  const [personaId, setPersonaId] = useState(personas[0]?.id ?? '');
  const { session } = useEmbedSession(scenarioId, personaId);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  const log = useEventLog();
  const [selection, setSelection] = useState<SigmaEvent | null>(null);
  const [nodeId, setNodeId] = useState('');
  const [bookmark, setBookmark] = useState('');
  const [bookmarkId, setBookmarkId] = useState('');
  const cmd = commands(iframeRef);

  const onEvent = (e: SigmaEvent) => {
    log.push(e);
    if (SELECTION_EVENTS.includes(e.name)) setSelection(e);
    if (e.name === 'workbook:bookmark:oncreate') setBookmarkId((e.payload as { bookmarkId: string }).bookmarkId);
  };

  return (
    <>
      <PageHeader title="Events & Interactions">
        Click a table cell, pivot cell or chart mark inside the embed and your app receives the underlying data, ready to power a native drill-down, a side panel, or a deep link. Commands flow the other way too.
      </PageHeader>
      <div className="split">
        <aside className="side">
          <Card title="Source">
            <ScenarioSelect value={scenarioId} onChange={setScenarioId} />
            <PersonaSelect value={personaId} onChange={setPersonaId} />
          </Card>
          <Card title="Host app: last selection">
            {selection ? (
              <>
                <p className="muted"><code>{selection.name}</code></p>
                <Json value={selection.payload} />
              </>
            ) : <p className="muted">Nothing selected yet.</p>}
          </Card>
          <Card title="Commands">
            <Field label="Element or page node ID">
              <input value={nodeId} onChange={(e) => setNodeId(e.target.value)} placeholder="from workbook:selectednodeid:onchange" />
            </Field>
            <div className="row">
              <button className="btn small" disabled={!nodeId} onClick={() => cmd.fullscreen(nodeId)}>Fullscreen</button>
              <button className="btn small" onClick={() => cmd.fullscreen(null)}>Exit fullscreen</button>
              <button className="btn small" disabled={!nodeId} onClick={() => cmd.selectNode(nodeId, 'element')}>Select element</button>
              <button className="btn small" disabled={!nodeId} onClick={() => cmd.selectNode(nodeId, 'page')}>Select page</button>
            </div>
            <Field label="Bookmark name">
              <input value={bookmark} onChange={(e) => setBookmark(e.target.value)} />
            </Field>
            <div className="row">
              <button className="btn small" disabled={!bookmark}
                onClick={() => cmd.bookmarkCreate({ name: bookmark, isDefault: false, isShared: false })}>Create</button>
              <button className="btn small" onClick={() => cmd.bookmarkUpdate()}>Update current</button>
            </div>
            <Field label="Bookmark ID">
              <input value={bookmarkId} onChange={(e) => setBookmarkId(e.target.value)} />
            </Field>
            <div className="row">
              <button className="btn small" disabled={!bookmarkId} onClick={() => cmd.bookmarkSelect(bookmarkId)}>Select</button>
              <button className="btn small" disabled={!bookmarkId} onClick={() => cmd.bookmarkDelete(bookmarkId)}>Delete</button>
            </div>
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
