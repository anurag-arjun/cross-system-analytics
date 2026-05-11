/** Compact number / time / address formatters used across charts and tables. */

const COMPACT = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 });
const FULL = new Intl.NumberFormat('en-US');
const PCT = new Intl.NumberFormat('en-US', { style: 'percent', maximumFractionDigits: 1 });

export const compact = (n: number) => COMPACT.format(n);
export const full = (n: number) => FULL.format(n);
export const pct = (n: number) => PCT.format(n);

export const ratio = (n: number) => (n >= 100 ? `${COMPACT.format(n)}x` : `${n.toFixed(1)}x`);

export function shortAddr(addr: string): string {
  if (!addr) return '';
  if (addr.length <= 14) return addr;
  return `${addr.slice(0, 8)}…${addr.slice(-4)}`;
}

export function durationFromSeconds(s: number): string {
  if (s < 60) return `${Math.round(s)}s`;
  if (s < 3600) return `${Math.round(s / 60)}m`;
  if (s < 86400) return `${(s / 3600).toFixed(1)}h`;
  return `${(s / 86400).toFixed(1)}d`;
}
