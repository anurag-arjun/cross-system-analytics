/**
 * Small set of presentation primitives — KPI cards, tables, banners.
 * Keeps the page code declarative without pulling in shadcn's full
 * generator (Tailwind utility classes are enough at this size).
 */

import { clsx } from 'clsx';
import type { ReactNode } from 'react';

export function Card({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <div
      className={clsx(
        'rounded-lg bg-[var(--color-panel)] border border-[var(--color-border)] p-4',
        className,
      )}
    >
      {children}
    </div>
  );
}

export function Kpi({
  label,
  value,
  hint,
  accent,
}: {
  label: string;
  value: string | number;
  hint?: string;
  accent?: 'extreme' | 'high' | 'accent';
}) {
  const accentColor =
    accent === 'extreme'
      ? 'var(--color-alert-extreme)'
      : accent === 'high'
        ? 'var(--color-alert-high)'
        : accent === 'accent'
          ? 'var(--color-accent)'
          : undefined;
  return (
    <Card className="flex flex-col gap-1">
      <div className="text-xs uppercase tracking-wider text-[var(--color-muted)]">{label}</div>
      <div
        className="text-3xl font-semibold tabular leading-tight"
        style={accentColor ? { color: accentColor } : undefined}
      >
        {value}
      </div>
      {hint && <div className="text-xs text-[var(--color-muted)]">{hint}</div>}
    </Card>
  );
}

export function Section({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle?: string;
  children: ReactNode;
}) {
  return (
    <section className="mt-8">
      <h2 className="text-lg font-semibold mb-1">{title}</h2>
      {subtitle && <p className="text-sm text-[var(--color-muted)] mb-3">{subtitle}</p>}
      {children}
    </section>
  );
}

export function Banner({ children }: { children: ReactNode }) {
  return (
    <div className="rounded-md border border-[var(--color-border)] bg-[var(--color-panel-2)] px-4 py-3 text-sm text-[var(--color-muted)]">
      {children}
    </div>
  );
}

export function Table<T>({
  data,
  columns,
  empty = 'No rows.',
}: {
  data: T[];
  columns: {
    key: keyof T | string;
    label: string;
    render?: (row: T) => ReactNode;
    align?: 'left' | 'right';
    width?: string;
  }[];
  empty?: string;
}) {
  if (!data?.length) {
    return <div className="text-sm text-[var(--color-muted)] py-6 text-center">{empty}</div>;
  }
  return (
    <div className="overflow-x-auto -mx-1">
      <table className="w-full text-sm border-collapse">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wider text-[var(--color-muted)]">
            {columns.map((c, i) => (
              <th
                key={i}
                className={clsx('px-3 py-2 font-medium', c.align === 'right' && 'text-right')}
                style={c.width ? { width: c.width } : undefined}
              >
                {c.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {data.map((row, i) => (
            <tr key={i} className="border-t border-[var(--color-border)]">
              {columns.map((c, j) => (
                <td
                  key={j}
                  className={clsx(
                    'px-3 py-2 tabular',
                    c.align === 'right' && 'text-right',
                  )}
                >
                  {c.render ? c.render(row) : (row as Record<string, unknown>)[c.key as string] as ReactNode}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function Loading({ label = 'Loading…' }: { label?: string }) {
  return <div className="py-6 text-center text-sm text-[var(--color-muted)]">{label}</div>;
}

export function ErrorBox({ error }: { error: unknown }) {
  const msg = error instanceof Error ? error.message : String(error);
  return (
    <div className="rounded-md border border-[var(--color-alert-extreme)]/40 bg-[var(--color-alert-extreme)]/10 px-3 py-2 text-sm text-[var(--color-alert-extreme)]">
      {msg}
    </div>
  );
}
