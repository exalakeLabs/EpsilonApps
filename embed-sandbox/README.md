# Embed Sandbox

A portal that showcases Sigma embedding, built on [`@sigmacomputing/embed-sdk`](https://www.npmjs.com/package/@sigmacomputing/embed-sdk).
React + Vite front end, small Express server that signs secure-embed JWTs.

## Quick start

```bash
npm install
cp .env.example .env                           # embed client ID + secret
cp portal.config.example.json portal.config.json   # your workbook URLs and personas
npm run dev                                    # web on :5173, API on :3001
```

No credentials yet? Open **Playground** and paste any public Sigma embed URL.

## Showcase pages

| Page | Capability |
| --- | --- |
| Layout & Theming | Embed URL params: theme, menu position, language, tooltips, responsive height |
| Host-driven Controls | Read/write workbook control values (`workbookVariablesUpdate`, `workbook:variables:onchange`) |
| Events & Interactions | Table/pivot/chart selection events, fullscreen, node selection, bookmarks, live event log |
| Multi-tenant Security | Per-viewer JWTs with `teams`, `account_type`, `user_attributes`; side-by-side compare |
| Playground | Paste an embed URL and inspect all SDK events |

## Configuration

- `.env`: `SIGMA_CLIENT_ID`, `SIGMA_EMBED_SECRET` (Administration > Developer Access > Embed secrets), `SESSION_SECONDS`, `PORT`. Server-only.
- `portal.config.json` (git-ignored): `scenarios` (workbook embed URLs, `secure` or `public`) and `personas` (identities the server may sign for).

The browser only ever sends a scenario id and persona id. The server resolves the base URL and identity, so visitors cannot mint tokens for other users or workbooks.

## Layout

```
server/   Express API: /api/config, /api/embed-url (jwt.ts signs HS256 tokens)
src/sigma/  SDK wiring: events.ts (all listeners + commands), SigmaFrame.tsx
src/pages/  One file per showcase
```

Add a showcase: create a page in `src/pages`, register it in `ROUTES` in `src/App.tsx`.

## Notes

- Control IDs must be set on workbook controls for them to appear on the Host-driven Controls page.
- The SDK's typings declare `*EventName` constants that are not exported at runtime in 0.7.1, so `events.ts` uses string literals.
- Verify JWT claim names and URL parameters against Sigma's current embed docs for your org; they are the part most likely to vary by version.
- Production: `npm run build && npm start` serves `dist/` and the API from one process.
