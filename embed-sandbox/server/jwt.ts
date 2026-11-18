import { randomUUID } from 'node:crypto';
import jwt from 'jsonwebtoken';
import type { Persona } from './config.js';

export interface SignResult {
  url: string;
  expiresAt: number;
  /** Claims as sent to Sigma (never includes the secret). */
  claims: Record<string, unknown>;
}

/**
 * Builds a Sigma secure-embed URL: a base workbook URL plus a short-lived HS256 JWT.
 * The JWT `kid` header and `iss` claim carry the embed client ID.
 */
export function signEmbedUrl(opts: {
  baseUrl: string;
  clientId: string;
  secret: string;
  persona: Persona;
  ttlSeconds: number;
}): SignResult {
  const { baseUrl, clientId, secret, persona, ttlSeconds } = opts;

  const claims: Record<string, unknown> = {
    sub: persona.email,
    jti: randomUUID(),
    first_name: persona.firstName,
    last_name: persona.lastName,
    account_type: persona.accountType,
    teams: persona.teams,
    user_attributes: persona.userAttributes,
  };
  for (const k of Object.keys(claims)) if (claims[k] === undefined) delete claims[k];

  const token = jwt.sign(claims, secret, {
    algorithm: 'HS256',
    keyid: clientId,
    issuer: clientId,
    expiresIn: ttlSeconds,
  });

  const decoded = jwt.decode(token) as Record<string, unknown>;
  const sep = baseUrl.includes('?') ? '&' : '?';
  return {
    url: `${baseUrl}${sep}:jwt=${token}&:embed=true`,
    expiresAt: (decoded.exp as number) * 1000,
    claims: decoded,
  };
}
