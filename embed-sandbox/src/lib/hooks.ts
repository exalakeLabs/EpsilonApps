import { createContext, useCallback, useContext, useEffect, useState } from 'react';
import { getEmbedUrl, type EmbedUrl, type PortalInfo } from './api';
import type { SigmaEvent } from '../sigma/events';

export const PortalContext = createContext<PortalInfo | null>(null);
export const usePortal = () => {
  const v = useContext(PortalContext);
  if (!v) throw new Error('PortalContext missing');
  return v;
};

export function useHashRoute(fallback: string): [string, (r: string) => void] {
  const read = () => window.location.hash.replace(/^#\/?/, '') || fallback;
  const [route, setRoute] = useState(read);
  useEffect(() => {
    const on = () => setRoute(read());
    window.addEventListener('hashchange', on);
    return () => window.removeEventListener('hashchange', on);
  }, []);
  return [route, (r) => { window.location.hash = `/${r}`; }];
}

export type Session =
  | { status: 'loading' }
  | { status: 'error'; error: string }
  | ({ status: 'ready' } & EmbedUrl);

/** Fetches a signed embed URL for a scenario/persona pair; `reload()` mints a fresh token. */
export function useEmbedSession(scenarioId: string | undefined, personaId?: string) {
  const [session, setSession] = useState<Session>({ status: 'loading' });
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    if (!scenarioId) return;
    let cancelled = false;
    setSession({ status: 'loading' });
    getEmbedUrl(scenarioId, personaId)
      .then((r) => !cancelled && setSession({ status: 'ready', ...r }))
      .catch((e: Error) => !cancelled && setSession({ status: 'error', error: e.message }));
    return () => { cancelled = true; };
  }, [scenarioId, personaId, nonce]);

  return { session, reload: useCallback(() => setNonce((n) => n + 1), []) };
}

export function useEventLog(limit = 300) {
  const [events, setEvents] = useState<SigmaEvent[]>([]);
  const [paused, setPaused] = useState(false);
  const push = useCallback(
    (e: SigmaEvent) => setEvents((prev) => (paused ? prev : [e, ...prev].slice(0, limit))),
    [paused, limit],
  );
  return { events, push, clear: () => setEvents([]), paused, setPaused };
}
