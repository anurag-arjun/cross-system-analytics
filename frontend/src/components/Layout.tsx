/**
 * Top nav + main content shell. Dark theme.
 */

import { NavLink, Outlet } from 'react-router-dom';
import { Activity, ArrowRightLeft } from 'lucide-react';

export function Layout() {
  return (
    <div className="min-h-screen flex flex-col">
      <header className="border-b border-[var(--color-border)] bg-[var(--color-panel)]">
        <div className="max-w-7xl mx-auto px-6 py-3 flex items-center gap-6">
          <div className="font-semibold tracking-tight text-[var(--color-accent)]">
            Nexus Analytics
          </div>
          <nav className="flex gap-1 ml-4 text-sm">
            <Tab to="/bridge" icon={<ArrowRightLeft size={14} />}>
              Bridge Flow
            </Tab>
            <Tab to="/spikes" icon={<Activity size={14} />}>
              Spike Detection
            </Tab>
          </nav>
          <div className="ml-auto text-xs text-[var(--color-muted)]">BD MVP · v0.1</div>
        </div>
      </header>
      <main className="flex-1 max-w-7xl w-full mx-auto px-6 py-6">
        <Outlet />
      </main>
    </div>
  );
}

function Tab({
  to,
  icon,
  children,
}: {
  to: string;
  icon: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `inline-flex items-center gap-1.5 px-3 py-1.5 rounded transition ${
          isActive
            ? 'bg-[var(--color-accent-bg)] text-[var(--color-accent)]'
            : 'text-[var(--color-muted)] hover:text-[var(--color-text)]'
        }`
      }
    >
      {icon}
      {children}
    </NavLink>
  );
}
