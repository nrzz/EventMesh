import type { ReactNode } from 'react';
import { NavLink, Outlet } from 'react-router-dom';
import { SignalRProvider, useSignalRContext } from '../context/SignalRContext';

interface NavItem {
  to: string;
  label: string;
  icon: string;
}

const navItems: NavItem[] = [
  { to: '/', label: 'Overview', icon: '◉' },
  { to: '/connections', label: 'Connections', icon: '⬡' },
  { to: '/topics', label: 'Topics', icon: '▤' },
  { to: '/queues', label: 'Queues', icon: '▦' },
  { to: '/messages', label: 'Messages', icon: '✉' },
  { to: '/consumers', label: 'Consumers', icon: '◎' },
  { to: '/retries', label: 'Retries', icon: '↻' },
  { to: '/dead-letters', label: 'Dead Letters', icon: '⚠' },
  { to: '/replay', label: 'Replay', icon: '⏮' },
  { to: '/metrics', label: 'Metrics', icon: '▥' },
  { to: '/tracing', label: 'Tracing', icon: '⌁' },
  { to: '/plugins', label: 'Plugins', icon: '⚙' },
  { to: '/cluster-health', label: 'Cluster Health', icon: '♥' },
  { to: '/settings', label: 'Settings', icon: '☰' },
];

function StatusBadge({ connected }: { connected: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${
        connected
          ? 'bg-emerald-500/15 text-emerald-400 ring-1 ring-emerald-500/30'
          : 'bg-amber-500/15 text-amber-400 ring-1 ring-amber-500/30'
      }`}
    >
      <span className={`h-1.5 w-1.5 rounded-full ${connected ? 'bg-emerald-400' : 'bg-amber-400'}`} />
      {connected ? 'Live' : 'Offline'}
    </span>
  );
}

export function Layout({ children }: { children?: ReactNode }) {
  return (
    <SignalRProvider>
      <LayoutContent>{children}</LayoutContent>
    </SignalRProvider>
  );
}

function LayoutContent({ children }: { children?: ReactNode }) {
  const signalR = useSignalRContext();

  return (
    <div className="flex min-h-screen bg-slate-950">
      <aside className="flex w-64 shrink-0 flex-col border-r border-slate-800 bg-slate-900/80 backdrop-blur">
        <div className="border-b border-slate-800 px-5 py-5">
          <div className="flex items-center gap-3">
            <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-mesh-500 text-sm font-bold text-white shadow-lg shadow-mesh-500/20">
              EM
            </div>
            <div>
              <h1 className="text-sm font-semibold text-white">EventMesh</h1>
              <p className="text-xs text-slate-400">Operations Dashboard</p>
            </div>
          </div>
          <div className="mt-4">
            <StatusBadge connected={signalR.connected} />
          </div>
        </div>

        <nav className="flex-1 overflow-y-auto px-3 py-4">
          <ul className="space-y-0.5">
            {navItems.map((item) => (
              <li key={item.to}>
                <NavLink
                  to={item.to}
                  end={item.to === '/'}
                  className={({ isActive }) =>
                    `flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition-colors ${
                      isActive
                        ? 'bg-mesh-500/15 text-mesh-300 ring-1 ring-mesh-500/30'
                        : 'text-slate-400 hover:bg-slate-800/80 hover:text-slate-200'
                    }`
                  }
                >
                  <span className="w-4 text-center text-xs opacity-70">{item.icon}</span>
                  {item.label}
                </NavLink>
              </li>
            ))}
          </ul>
        </nav>

        <div className="border-t border-slate-800 px-5 py-4 text-xs text-slate-500">
          v0.1.0 · Control Plane
        </div>
      </aside>

      <main className="flex min-w-0 flex-1 flex-col">
        <header className="border-b border-slate-800 bg-slate-900/40 px-8 py-4 backdrop-blur">
          <div className="flex items-center justify-between">
            <p className="text-sm text-slate-400">
              Real-time mesh observability and operations
            </p>
            {signalR.lastEventAt && (
              <p className="font-mono text-xs text-slate-500">
                Last update: {new Date(signalR.lastEventAt).toLocaleTimeString()}
              </p>
            )}
          </div>
        </header>

        <div className="flex-1 overflow-auto p-8">{children ?? <Outlet />}</div>
      </main>
    </div>
  );
}
