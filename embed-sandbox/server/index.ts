import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import express from 'express';
import { isScenarioConfigured, loadPortalConfig } from './config.js';
import { signEmbedUrl } from './jwt.js';

const app = express();
app.use(express.json());

const clientId = process.env.SIGMA_CLIENT_ID ?? '';
const secret = process.env.SIGMA_EMBED_SECRET ?? '';
const hasCredentials = Boolean(clientId && secret);
const ttl = Math.min(Math.max(Number(process.env.SESSION_SECONDS) || 600, 60), 3600);

app.get('/api/config', (_req, res) => {
  const { config, source } = loadPortalConfig();
  res.json({
    source,
    hasCredentials,
    sessionSeconds: ttl,
    // Embed URLs are intentionally not exposed; the browser only learns ids and labels.
    scenarios: config.scenarios.map((s) => ({
      id: s.id,
      title: s.title,
      summary: s.summary,
      auth: s.auth,
      configured: isScenarioConfigured(s) && (s.auth === 'public' || hasCredentials),
    })),
    personas: config.personas.map((p) => ({
      id: p.id,
      label: p.label,
      email: p.email,
      accountType: p.accountType,
      teams: p.teams,
      userAttributes: p.userAttributes,
    })),
  });
});

// The client sends only ids. Base URL, identity and attributes are resolved server-side,
// so a visitor can never mint a token for an arbitrary user or workbook.
app.post('/api/embed-url', (req, res) => {
  const { scenarioId, personaId } = req.body ?? {};
  const { config } = loadPortalConfig();

  const scenario = config.scenarios.find((s) => s.id === scenarioId);
  if (!scenario) return res.status(404).json({ error: `Unknown scenario "${scenarioId}"` });
  if (!isScenarioConfigured(scenario)) {
    return res.status(409).json({
      error: 'Scenario still has placeholder URL. Copy portal.config.example.json to portal.config.json and edit it.',
    });
  }

  if (scenario.auth === 'public') {
    return res.json({ url: scenario.embedUrl, expiresAt: null, claims: null });
  }

  if (!hasCredentials) {
    return res.status(409).json({ error: 'SIGMA_CLIENT_ID and SIGMA_EMBED_SECRET are not set. See .env.example.' });
  }
  const persona = config.personas.find((p) => p.id === (personaId ?? config.personas[0]?.id));
  if (!persona) return res.status(404).json({ error: `Unknown persona "${personaId}"` });

  res.json(
    signEmbedUrl({ baseUrl: scenario.embedUrl, clientId, secret, persona, ttlSeconds: ttl }),
  );
});

const dist = path.join(process.cwd(), 'dist');
if (process.env.NODE_ENV === 'production' && fs.existsSync(dist)) {
  app.use(express.static(dist));
  app.get(/^(?!\/api).*/, (_req, res) => res.sendFile(path.join(dist, 'index.html')));
}

const port = Number(process.env.PORT) || 3001;
app.listen(port, () => {
  console.log(`API listening on http://localhost:${port} (credentials ${hasCredentials ? 'loaded' : 'missing'})`);
});
