import { useEffect, useState } from 'react';
import { getPortalInfo, type PortalInfo } from './lib/api';
import { PortalContext, useHashRoute } from './lib/hooks';
import { Home } from './pages/Home';
import { Basics } from './pages/Basics';
import { Variables } from './pages/Variables';
import { Interactions } from './pages/Interactions';
import { MultiTenant } from './pages/MultiTenant';
import { Playground } from './pages/Playground';

const ROUTES: Record<string, { label: string; el: () => React.ReactElement }> = {
  home: { label: 'Overview', el: Home },
  basics: { label: 'Layout & Theming', el: Basics },
  variables: { label: 'Host-driven Controls', el: Variables },
  interactions: { label: 'Events & Interactions', el: Interactions },
  'multi-tenant': { label: 'Multi-tenant Security', el: MultiTenant },
  playground: { label: 'Playground', el: Playground },
};

export function App() {
  const [route] = useHashRoute('home');
  const [info, setInfo] = useState<PortalInfo | null>(null);
  const [error, setError] = useState('');

  useEffect(() => {
    getPortalInfo().then(setInfo).catch((e: Error) => setError(e.message));
  }, []);

  const Page = (ROUTES[route] ?? ROUTES.home).el;

  return (
    <div className="shell">
      <nav className="nav">
        <div className="brand">Embed Sandbox</div>
        {Object.entries(ROUTES).map(([k, r]) => (
          <a key={k} href={`#/${k}`} className={k === route ? 'active' : ''}>{r.label}</a>
        ))}
      </nav>
      <div className="content">
        {error && <div className="stage-empty error">API unreachable: {error}. Run <code>npm run dev</code> to start both servers.</div>}
        {!info && !error && <div className="stage-empty">Loading…</div>}
        {info && <PortalContext.Provider value={info}><Page /></PortalContext.Provider>}
      </div>
    </div>
  );
}
