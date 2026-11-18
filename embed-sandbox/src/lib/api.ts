export interface ScenarioInfo {
  id: string;
  title: string;
  summary: string;
  auth: 'secure' | 'public';
  configured: boolean;
}
export interface PersonaInfo {
  id: string;
  label: string;
  email: string;
  accountType?: string;
  teams?: string[];
  userAttributes?: Record<string, string>;
}
export interface PortalInfo {
  source: string;
  hasCredentials: boolean;
  sessionSeconds: number;
  scenarios: ScenarioInfo[];
  personas: PersonaInfo[];
}
export interface EmbedUrl {
  url: string;
  expiresAt: number | null;
  claims: Record<string, unknown> | null;
}

async function json<T>(res: Response): Promise<T> {
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((body as { error?: string }).error ?? `HTTP ${res.status}`);
  return body as T;
}

export const getPortalInfo = () => fetch('/api/config').then(json<PortalInfo>);

export const getEmbedUrl = (scenarioId: string, personaId?: string) =>
  fetch('/api/embed-url', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ scenarioId, personaId }),
  }).then(json<EmbedUrl>);
