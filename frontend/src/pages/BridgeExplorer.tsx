/**
 * Bridge Explorer — per-transaction view of bridge_out / bridge_in pairs
 * and orphans, with a per-bridge punch-list summary at the top.
 *
 * Rows are pre-classified server-side; this page is a layout + filter
 * shell over the explorer endpoint.
 */

import { useQuery } from '@tanstack/react-query';
import { useEffect, useMemo, useState } from 'react';

import { Banner, Card, ErrorBox, Loading, Section, Table } from '@/components/ui';
import {
  fetchExplorer,
  type BridgeStatus,
  type BridgeTag,
  type ExplorerRow,
} from '@/lib/api';
import { durationFromSeconds, full, shortAddr } from '@/lib/format';

// ---------------------------------------------------------------------------
// Status / tag presentation
// ---------------------------------------------------------------------------

const STATUS_PRESENT: Record<BridgeStatus, { label: string; tone: string }> = {
  MATCHED:                    { label: 'matched',         tone: 'bg-emerald-100 text-emerald-800 border-emerald-300' },
  PENDING_FINALITY:           { label: 'pending finality', tone: 'bg-amber-100 text-amber-800 border-amber-300' },
  IN_FLIGHT:                  { label: 'in flight',        tone: 'bg-amber-100 text-amber-800 border-amber-300' },
  UNMATCHED_DST_OUT_OF_SCOPE: { label: 'dst out of scope', tone: 'bg-zinc-100 text-zinc-700 border-zinc-300' },
  UNMATCHED_SRC_OUT_OF_SCOPE: { label: 'src out of scope', tone: 'bg-zinc-100 text-zinc-700 border-zinc-300' },
  UNMATCHED_DECODER_GAP:      { label: 'decoder gap',      tone: 'bg-rose-100 text-rose-800 border-rose-300' },
  UNMATCHED_BROKEN_MATCHER:   { label: 'broken matcher',   tone: 'bg-rose-100 text-rose-800 border-rose-300' },
  UNMATCHED_UNKNOWN:          { label: 'unknown',          tone: 'bg-rose-100 text-rose-800 border-rose-300' },
};

const TAG_LABELS: Record<BridgeTag, string> = {
  AMOUNT_MISMATCH:   'Δ amount',
  LATENCY_OUTLIER:   'Δ latency',
  NEGATIVE_LATENCY:  '−latency',
  SAME_CHAIN:        'same-chain',
  MULTI_MATCH:       'multi-match',
  RECIPIENT_DIFFERS: 'wallet→wallet',
  NO_USD_VALUE:      'no $',
  TOKEN_CHANGED:     'token Δ',
};

const STATUS_OPTIONS: BridgeStatus[] = [
  'MATCHED',
  'PENDING_FINALITY',
  'IN_FLIGHT',
  'UNMATCHED_DST_OUT_OF_SCOPE',
  'UNMATCHED_SRC_OUT_OF_SCOPE',
  'UNMATCHED_DECODER_GAP',
  'UNMATCHED_BROKEN_MATCHER',
  'UNMATCHED_UNKNOWN',
];

const WINDOWS = [
  { hours: 24,  label: '24h' },
  { hours: 168, label: '7d'  },
  { hours: 336, label: '14d' },
];

const PAGE_SIZES = [20, 50, 100] as const;
type PageSize = (typeof PAGE_SIZES)[number];

const CHAINS = ['ethereum', 'base', 'arbitrum', 'optimism', 'polygon'] as const;

// ---------------------------------------------------------------------------
// Page
// ---------------------------------------------------------------------------

export function BridgeExplorerPage() {
  const [hours, setHours] = useState(24);
  const [statusFilter, setStatusFilter] = useState<Set<BridgeStatus>>(new Set());
  const [chainFilter, setChainFilter] = useState<Set<string>>(new Set());
  const [bridgeFilter, setBridgeFilter] = useState<Set<string>>(new Set());
  const [pageSize, setPageSize] = useState<PageSize>(20);
  const [page, setPage] = useState(0);

  const { data, isLoading, error } = useQuery({
    queryKey: ['explorer', hours],
    queryFn: () => fetchExplorer(hours, { limit: 2000 }),
  });

  // Reset to page 0 whenever the filter set or window changes — otherwise
  // we'd land on a page that's now empty.
  useEffect(() => {
    setPage(0);
  }, [statusFilter, chainFilter, bridgeFilter, hours, pageSize]);

  // Client-side filter so the user can flip statuses without re-querying.
  const filteredRows = useMemo(() => {
    if (!data) return [];
    return data.rows.filter((r) => {
      if (statusFilter.size > 0 && !statusFilter.has(r.status)) return false;
      if (chainFilter.size > 0) {
        const matches =
          (r.src_chain && chainFilter.has(r.src_chain)) ||
          (r.dst_chain && chainFilter.has(r.dst_chain));
        if (!matches) return false;
      }
      if (bridgeFilter.size > 0 && !bridgeFilter.has(r.bridge)) return false;
      return true;
    });
  }, [data, statusFilter, chainFilter, bridgeFilter]);

  const allBridges = useMemo(
    () => Object.keys(data?.summary ?? {}).sort(),
    [data],
  );

  const pageCount = Math.max(1, Math.ceil(filteredRows.length / pageSize));
  const safePage = Math.min(page, pageCount - 1);
  const pagedRows = filteredRows.slice(safePage * pageSize, (safePage + 1) * pageSize);

  return (
    <div>
      <header className="mb-4">
        <h1 className="text-2xl font-semibold">Bridge Explorer</h1>
        <p className="text-sm text-[var(--color-muted)]">
          Every bridge_out and bridge_in over the window. Matched pairs, orphans, and the reason each orphan stayed unmatched.
        </p>
      </header>

      {/* Window selector */}
      <div className="flex gap-1 rounded-md bg-[var(--color-panel)] border border-[var(--color-border)] p-1 w-fit mb-4">
        {WINDOWS.map((w) => (
          <button
            key={w.hours}
            onClick={() => setHours(w.hours)}
            className={`px-3 py-1 rounded text-sm transition ${
              hours === w.hours
                ? 'bg-[var(--color-accent-bg)] text-[var(--color-accent)]'
                : 'text-[var(--color-muted)] hover:text-[var(--color-text)]'
            }`}
          >
            {w.label}
          </button>
        ))}
      </div>

      {isLoading && <Loading />}
      {error && <ErrorBox error={error} />}

      {data && (
        <>
          <Section
            title="Punch list"
            subtitle="Per-bridge breakdown of where transactions ended up. Green = matched, amber = expected wait, red = decoder/matcher gap or unexplained."
          >
            <PunchList summary={data.summary} />
          </Section>

          <Section
            title="Transactions"
            subtitle={`${full(filteredRows.length)} of ${full(data.row_count)} rows`}
          >
            <FilterBar
              statuses={statusFilter}
              setStatuses={setStatusFilter}
              chains={chainFilter}
              setChains={setChainFilter}
              bridges={bridgeFilter}
              setBridges={setBridgeFilter}
              allBridges={allBridges}
            />
            <Card className="mt-3 p-0 overflow-hidden">
              <RowTable rows={pagedRows} />
            </Card>
            <Pager
              page={safePage}
              pageCount={pageCount}
              pageSize={pageSize}
              setPage={setPage}
              setPageSize={setPageSize}
              totalRows={filteredRows.length}
            />
          </Section>

          <Banner>
            <div className="text-xs">
              Pre-classified server-side via{' '}
              <code className="font-mono">core/identity/bridge_status.py</code>. Row source: UNION of <code>bridge_links FINAL</code> +
              orphan bridge_outs + orphan bridge_ins from <code>canonical_events FINAL</code>.
            </div>
          </Banner>
        </>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Punch-list summary cards
// ---------------------------------------------------------------------------

function PunchList({
  summary,
}: {
  summary: Record<string, Partial<Record<BridgeStatus, number>>>;
}) {
  const bridges = Object.entries(summary).sort((a, b) => {
    const ta = sum(a[1]);
    const tb = sum(b[1]);
    return tb - ta;
  });
  if (!bridges.length) {
    return <div className="text-sm text-[var(--color-muted)]">No bridge activity in window.</div>;
  }
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
      {bridges.map(([bridge, statuses]) => (
        <PunchListCard key={bridge} bridge={bridge} statuses={statuses} />
      ))}
    </div>
  );
}

function PunchListCard({
  bridge,
  statuses,
}: {
  bridge: string;
  statuses: Partial<Record<BridgeStatus, number>>;
}) {
  const total = sum(statuses);
  const matched = statuses.MATCHED ?? 0;
  const pendingOrInflight = (statuses.PENDING_FINALITY ?? 0) + (statuses.IN_FLIGHT ?? 0);
  const outOfScope =
    (statuses.UNMATCHED_DST_OUT_OF_SCOPE ?? 0) + (statuses.UNMATCHED_SRC_OUT_OF_SCOPE ?? 0);
  const decoderOrMatcher =
    (statuses.UNMATCHED_DECODER_GAP ?? 0) + (statuses.UNMATCHED_BROKEN_MATCHER ?? 0);
  const unknown = statuses.UNMATCHED_UNKNOWN ?? 0;

  // Health summary in a single emoji-free icon.
  const health =
    decoderOrMatcher > 0 ? '🔴' : unknown > 0 || matched < total / 2 ? '⚠️' : '🟢';

  return (
    <Card>
      <div className="flex items-baseline justify-between">
        <div>
          <div className="text-sm font-semibold capitalize">{bridge.replaceAll('_', ' ')}</div>
          <div className="text-xs text-[var(--color-muted)]">{full(total)} txs</div>
        </div>
        <div className="text-lg">{health}</div>
      </div>
      <ul className="mt-2 space-y-1 text-xs">
        {matched > 0           && <Line label="matched"        n={matched}          tone="text-emerald-700" />}
        {pendingOrInflight > 0 && <Line label="in flight"      n={pendingOrInflight} tone="text-amber-700" />}
        {outOfScope > 0        && <Line label="out of scope"   n={outOfScope}        tone="text-zinc-600" />}
        {(statuses.UNMATCHED_DECODER_GAP ?? 0)   > 0 && <Line label="decoder gap"   n={statuses.UNMATCHED_DECODER_GAP ?? 0}    tone="text-rose-700" />}
        {(statuses.UNMATCHED_BROKEN_MATCHER ?? 0) > 0 && <Line label="broken matcher" n={statuses.UNMATCHED_BROKEN_MATCHER ?? 0} tone="text-rose-700" />}
        {unknown > 0           && <Line label="unknown"        n={unknown}           tone="text-rose-700" />}
      </ul>
    </Card>
  );
}

function Line({ label, n, tone }: { label: string; n: number; tone: string }) {
  return (
    <li className="flex justify-between">
      <span className={tone}>{label}</span>
      <span className="tabular text-[var(--color-muted)]">{full(n)}</span>
    </li>
  );
}

function sum(o: Partial<Record<BridgeStatus, number>>) {
  return Object.values(o).reduce((a, b) => (a ?? 0) + (b ?? 0), 0) ?? 0;
}

// ---------------------------------------------------------------------------
// Filter bar
// ---------------------------------------------------------------------------

function FilterBar({
  statuses,
  setStatuses,
  chains,
  setChains,
  bridges,
  setBridges,
  allBridges,
}: {
  statuses: Set<BridgeStatus>;
  setStatuses: (s: Set<BridgeStatus>) => void;
  chains: Set<string>;
  setChains: (s: Set<string>) => void;
  bridges: Set<string>;
  setBridges: (s: Set<string>) => void;
  allBridges: string[];
}) {
  return (
    <div className="flex flex-wrap gap-2 items-center">
      <ChipGroup
        label="status"
        items={STATUS_OPTIONS}
        labelFor={(s) => STATUS_PRESENT[s].label}
        selected={statuses}
        toggle={(s) => {
          const next = new Set(statuses);
          if (next.has(s)) next.delete(s);
          else next.add(s);
          setStatuses(next);
        }}
      />
      <ChipGroup
        label="chain"
        items={[...CHAINS]}
        labelFor={(c) => c}
        selected={chains}
        toggle={(c) => {
          const next = new Set(chains);
          if (next.has(c)) next.delete(c);
          else next.add(c);
          setChains(next);
        }}
      />
      <ChipGroup
        label="bridge"
        items={allBridges}
        labelFor={(b) => b}
        selected={bridges}
        toggle={(b) => {
          const next = new Set(bridges);
          if (next.has(b)) next.delete(b);
          else next.add(b);
          setBridges(next);
        }}
      />
    </div>
  );
}

function ChipGroup<T extends string>({
  label,
  items,
  labelFor,
  selected,
  toggle,
}: {
  label: string;
  items: T[];
  labelFor: (t: T) => string;
  selected: Set<T>;
  toggle: (t: T) => void;
}) {
  if (!items.length) return null;
  return (
    <div className="flex flex-wrap gap-1 items-center mr-3">
      <span className="text-xs uppercase tracking-wider text-[var(--color-muted)] mr-1">
        {label}
      </span>
      {items.map((it) => {
        const on = selected.has(it);
        return (
          <button
            key={it}
            onClick={() => toggle(it)}
            className={`px-2 py-0.5 rounded text-xs border transition ${
              on
                ? 'border-[var(--color-accent)] text-[var(--color-accent)] bg-[var(--color-accent-bg)]'
                : 'border-[var(--color-border)] text-[var(--color-muted)] hover:text-[var(--color-text)]'
            }`}
          >
            {labelFor(it)}
          </button>
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Transaction table
// ---------------------------------------------------------------------------

function RowTable({ rows }: { rows: ExplorerRow[] }) {
  return (
    <Table
      data={rows}
      empty="No transactions match the filters."
      columns={[
        {
          key: 'time',
          label: 'Time',
          render: (r) => {
            const t = r.src_block_time ?? r.dst_block_time;
            return <span className="text-xs text-[var(--color-muted)]">{formatTime(t)}</span>;
          },
        },
        {
          key: 'bridge',
          label: 'Bridge',
          render: (r) => <span className="capitalize">{r.bridge.replaceAll('_', ' ')}</span>,
        },
        {
          key: 'route',
          label: 'Route',
          render: (r) => (
            <div className="font-mono text-xs flex items-center gap-1">
              <span className={r.src_chain ? '' : 'text-[var(--color-muted)]'}>
                {r.src_chain ?? '—'}
              </span>
              <span className="text-[var(--color-muted)]">→</span>
              <span className={r.dst_chain ? '' : 'text-[var(--color-muted)]'}>
                {r.dst_chain ?? '—'}
              </span>
            </div>
          ),
        },
        {
          key: 'amount',
          label: 'Amount',
          align: 'right',
          render: (r) => {
            const usd = r.src_amount_usd ?? r.dst_amount_usd;
            if (usd != null) return <span className="tabular">${full(Math.round(usd))}</span>;
            return <span className="text-[var(--color-muted)] text-xs">—</span>;
          },
        },
        {
          key: 'latency',
          label: 'Latency',
          align: 'right',
          render: (r) =>
            r.latency_seconds != null ? (
              <span className="tabular text-xs">{durationFromSeconds(r.latency_seconds)}</span>
            ) : (
              <span className="text-[var(--color-muted)] text-xs">—</span>
            ),
        },
        {
          key: 'status',
          label: 'Status',
          render: (r) => {
            const p = STATUS_PRESENT[r.status];
            return (
              <div className="flex flex-wrap items-center gap-1">
                <span
                  className={`px-1.5 py-0.5 rounded text-xs border ${p.tone}`}
                  title={r.status_reason}
                >
                  {p.label}
                </span>
                {r.tags.map((t) => (
                  <span
                    key={t}
                    className="px-1.5 py-0.5 rounded text-xs border border-amber-300 bg-amber-100 text-amber-800"
                    title={t}
                  >
                    {TAG_LABELS[t]}
                  </span>
                ))}
              </div>
            );
          },
        },
        {
          key: 'reason',
          label: 'Detail',
          render: (r) => (
            <span className="text-xs text-[var(--color-muted)]" title={r.status_reason}>
              {r.status_reason || ' '}
            </span>
          ),
        },
        {
          key: 'tx',
          label: 'Tx',
          render: (r) => (
            <div className="font-mono text-xs flex flex-col">
              {r.src_tx_hash && <span title={r.src_tx_hash}>out: {shortAddr(r.src_tx_hash)}</span>}
              {r.dst_tx_hash && <span title={r.dst_tx_hash}>in: {shortAddr(r.dst_tx_hash)}</span>}
            </div>
          ),
        },
      ]}
    />
  );
}

// ---------------------------------------------------------------------------
// Pager
// ---------------------------------------------------------------------------

function Pager({
  page,
  pageCount,
  pageSize,
  setPage,
  setPageSize,
  totalRows,
}: {
  page: number;
  pageCount: number;
  pageSize: PageSize;
  setPage: (n: number) => void;
  setPageSize: (n: PageSize) => void;
  totalRows: number;
}) {
  const start = totalRows === 0 ? 0 : page * pageSize + 1;
  const end = Math.min((page + 1) * pageSize, totalRows);
  return (
    <div className="mt-3 flex flex-wrap items-center gap-3 text-sm">
      <span className="text-[var(--color-muted)] tabular">
        {full(start)}–{full(end)} of {full(totalRows)}
      </span>

      <div className="flex gap-1 rounded-md bg-[var(--color-panel)] border border-[var(--color-border)] p-1">
        <button
          onClick={() => setPage(Math.max(0, page - 1))}
          disabled={page === 0}
          className="px-2.5 py-1 text-sm rounded text-[var(--color-muted)] hover:text-[var(--color-text)] disabled:opacity-30 disabled:cursor-not-allowed"
        >
          ← Prev
        </button>
        <span className="px-2 py-1 text-xs tabular text-[var(--color-muted)] self-center">
          page {page + 1} / {pageCount}
        </span>
        <button
          onClick={() => setPage(Math.min(pageCount - 1, page + 1))}
          disabled={page >= pageCount - 1}
          className="px-2.5 py-1 text-sm rounded text-[var(--color-muted)] hover:text-[var(--color-text)] disabled:opacity-30 disabled:cursor-not-allowed"
        >
          Next →
        </button>
      </div>

      <div className="ml-auto flex items-center gap-2">
        <span className="text-xs uppercase tracking-wider text-[var(--color-muted)]">per page</span>
        <div className="flex gap-1 rounded-md bg-[var(--color-panel)] border border-[var(--color-border)] p-1">
          {PAGE_SIZES.map((n) => (
            <button
              key={n}
              onClick={() => setPageSize(n)}
              className={`px-2 py-0.5 rounded text-xs transition ${
                pageSize === n
                  ? 'bg-[var(--color-accent-bg)] text-[var(--color-accent)]'
                  : 'text-[var(--color-muted)] hover:text-[var(--color-text)]'
              }`}
            >
              {n}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

function formatTime(s: string | null): string {
  if (!s) return '—';
  const d = new Date(s);
  if (isNaN(d.getTime())) return s;
  // YYYY-MM-DD HH:MM in user's local time
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}
