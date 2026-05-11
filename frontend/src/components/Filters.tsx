/**
 * Top-of-page filters: chain dropdown + window selector.
 *
 * Window is "days" (1–30) since the BD doc views are time-windowed.
 * Both controls are uncontrolled in the URL (state lives in the page);
 * a future iteration could sync to query string for shareability.
 */

import type { Chain } from '@/lib/api';

interface Props {
  chain: Chain;
  setChain: (c: Chain) => void;
  days: number;
  setDays: (d: number) => void;
}

const CHAINS: Chain[] = ['all', 'ethereum', 'base', 'arbitrum', 'optimism', 'polygon'];
const WINDOWS = [
  { days: 1, label: '24 hours' },
  { days: 7, label: '7 days' },
  { days: 30, label: '30 days' },
];

export function Filters({ chain, setChain, days, setDays }: Props) {
  return (
    <div className="flex flex-wrap gap-3 items-center">
      <label className="text-xs uppercase tracking-wider text-[var(--color-muted)]">Window</label>
      <div className="flex gap-1 rounded-md bg-[var(--color-panel)] border border-[var(--color-border)] p-1">
        {WINDOWS.map((w) => (
          <button
            key={w.days}
            onClick={() => setDays(w.days)}
            className={`px-3 py-1 rounded text-sm transition ${
              days === w.days
                ? 'bg-[var(--color-accent-bg)] text-[var(--color-accent)]'
                : 'text-[var(--color-muted)] hover:text-[var(--color-text)]'
            }`}
          >
            {w.label}
          </button>
        ))}
      </div>

      <label className="ml-4 text-xs uppercase tracking-wider text-[var(--color-muted)]">Chain</label>
      <select
        value={chain}
        onChange={(e) => setChain(e.target.value as Chain)}
        className="bg-[var(--color-panel)] border border-[var(--color-border)] rounded-md px-3 py-1.5 text-sm capitalize"
      >
        {CHAINS.map((c) => (
          <option key={c} value={c} className="capitalize">
            {c === 'all' ? 'All chains' : c}
          </option>
        ))}
      </select>
    </div>
  );
}
