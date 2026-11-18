import { usePortal } from '../lib/hooks';
import { Card, PageHeader } from '../components/common';

const FEATURES = [
  { href: 'basics', title: 'Layout & Theming', body: 'Control menus, theme, language, tooltips and responsive height with embed URL parameters.' },
  { href: 'variables', title: 'Host-driven Controls', body: 'Read and write workbook control values from your own UI, two-way, in real time.' },
  { href: 'interactions', title: 'Events & Interactions', body: 'React to clicks on tables, pivots and charts; drive fullscreen, selection and bookmarks.' },
  { href: 'multi-tenant', title: 'Multi-tenant Security', body: 'Sign JWTs per user with teams and user attributes; compare two tenants side by side.' },
  { href: 'playground', title: 'Playground', body: 'Paste any embed URL and inspect every SDK event with no setup.' },
];

export function Home() {
  const info = usePortal();
  return (
    <>
      <PageHeader title="Sigma Embed Sandbox">
        A working reference portal for embedding Sigma with <code>@sigmacomputing/embed-sdk</code>. Each page isolates one capability, with live code paths you can lift into your product.
      </PageHeader>

      <div className="grid">
        {FEATURES.map((f) => (
          <a key={f.href} className="tile" href={`#/${f.href}`}>
            <h3>{f.title}</h3>
            <p>{f.body}</p>
          </a>
        ))}
      </div>

      <Card title="Setup status">
        <ul className="status">
          <li className={info.hasCredentials ? 'ok' : 'warn'}>
            Embed credentials: {info.hasCredentials ? 'loaded' : 'missing (set SIGMA_CLIENT_ID and SIGMA_EMBED_SECRET in .env)'}
          </li>
          <li className={info.source === 'portal.config.json' ? 'ok' : 'warn'}>
            Config file: {info.source}
            {info.source !== 'portal.config.json' && ' (using placeholders; copy to portal.config.json)'}
          </li>
          {info.scenarios.map((s) => (
            <li key={s.id} className={s.configured ? 'ok' : 'warn'}>
              {s.title} ({s.auth}): {s.configured ? 'ready' : 'not configured'}
            </li>
          ))}
        </ul>
      </Card>
    </>
  );
}
