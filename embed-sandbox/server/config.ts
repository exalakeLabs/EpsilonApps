import fs from 'node:fs';
import path from 'node:path';

export interface Scenario {
  id: string;
  title: string;
  summary: string;
  auth: 'secure' | 'public';
  embedUrl: string;
}

export interface Persona {
  id: string;
  label: string;
  email: string;
  firstName?: string;
  lastName?: string;
  accountType?: string;
  teams?: string[];
  userAttributes?: Record<string, string>;
}

export interface PortalConfig {
  scenarios: Scenario[];
  personas: Persona[];
}

const root = process.cwd();

export function loadPortalConfig(): { config: PortalConfig; source: string } {
  const real = path.join(root, 'portal.config.json');
  const example = path.join(root, 'portal.config.example.json');
  const file = fs.existsSync(real) ? real : example;
  const config = JSON.parse(fs.readFileSync(file, 'utf8')) as PortalConfig;
  return { config, source: path.basename(file) };
}

/** A scenario is usable once its URL no longer contains <placeholder> markers. */
export function isScenarioConfigured(s: Scenario): boolean {
  return !/[<>]/.test(s.embedUrl);
}
