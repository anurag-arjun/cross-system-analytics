/**
 * Bridge Flow Analytics page — matches section 1 of the BD requirements doc.
 *
 * Layout:
 *   row 1: KPI cards (bridge_ins, bridge_outs, swaps, non-swap DeFi, swap %)
 *   row 2: methodology banner
 *   row 3: swap-vs-non-swap bar chart  +  activity-after-bridge line chart
 *   row 4: first-action table          +  top-protocols bar
 *   row 5: second-hop table
 *   row 6: bridge breakdown table
 */

import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';

import { Filters } from '@/components/Filters';
import { Banner, Card, ErrorBox, Kpi, Loading, Section, Table } from '@/components/ui';
import {
  fetchActivity24h,
  fetchBridgeBreakdown,
  fetchBridgeCompletion,
  fetchBridgeSummary,
  fetchCrossChainMatrix,
  fetchFirstAction,
  fetchSecondHop,
  fetchSwapVsNonSwap,
  fetchTopProtocols,
  type Chain,
} from '@/lib/api';
import { compact, durationFromSeconds, full } from '@/lib/format';

const BAR_FILL = 'oklch(0.72 0.18 250)';
const BAR_ALT_FILL = 'oklch(0.74 0.20 70)';
const NO_ACTION_FILL = 'oklch(0.40 0.01 240)';

export function BridgeFlowPage() {
  const [chain, setChain] = useState<Chain>('all');
  const [days, setDays] = useState<number>(7);

  const summary = useQuery({
    queryKey: ['bridge-summary', days, chain],
    queryFn: () => fetchBridgeSummary(days, chain),
  });
  const breakdown = useQuery({
    queryKey: ['bridge-breakdown', days, chain],
    queryFn: () => fetchBridgeBreakdown(days, chain),
  });
  const firstAction = useQuery({
    queryKey: ['first-action', days, chain],
    queryFn: () => fetchFirstAction(days, chain),
  });
  const split = useQuery({
    queryKey: ['swap-vs-non-swap', days, chain],
    queryFn: () => fetchSwapVsNonSwap(days, chain),
  });
  const secondHop = useQuery({
    queryKey: ['second-hop', days, chain],
    queryFn: () => fetchSecondHop(days, chain),
  });
  const activity = useQuery({
    queryKey: ['activity-24h', days, chain],
    queryFn: () => fetchActivity24h(days, chain),
  });
  const crossChain = useQuery({
    queryKey: ['cross-chain-matrix', days, chain],
    queryFn: () => fetchCrossChainMatrix(days, chain),
  });
  const completion = useQuery({
    queryKey: ['bridge-completion', days, chain],
    queryFn: () => fetchBridgeCompletion(days, chain),
  });
  const topProtocols = useQuery({
    queryKey: ['top-protocols', days, chain],
    queryFn: () => fetchTopProtocols(days, chain),
  });

  return (
    <div>
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-bold">Bridge Flow Analytics</h1>
        <p className="text-sm text-[var(--color-muted)] max-w-3xl">
          What apps do users interact with after bridging into a chain? Each row of the
          first-action table is the median first <em>meaningful</em> DeFi action — token
          transfers and approvals are filtered out so what you see is the actual app they
          touched.
        </p>
        <Filters chain={chain} setChain={setChain} days={days} setDays={setDays} />
      </header>

      {summary.isError && <ErrorBox error={summary.error} />}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-6">
        <Kpi
          label="Bridge ins"
          value={summary.data ? compact(summary.data.bridge_ins) : '—'}
          hint={summary.data ? `${full(summary.data.bridge_ins)} events` : undefined}
          accent="accent"
        />
        <Kpi
          label="Bridge outs"
          value={summary.data ? compact(summary.data.bridge_outs) : '—'}
          hint={summary.data ? `${full(summary.data.bridge_outs)} events` : undefined}
        />
        <Kpi
          label="Swaps in window"
          value={summary.data ? compact(summary.data.swaps) : '—'}
          hint="all chains, swap event_type"
        />
        <Kpi
          label="Non-swap DeFi"
          value={summary.data ? compact(summary.data.non_swap_defi) : '—'}
          hint="lend / stake / lp / claim / perp / pool_create"
        />
      </div>

      <Section
        title="Cross-chain flow"
        subtitle="bridge_out matched to bridge_in via link_key. Latency in seconds."
      >
        <div className="grid lg:grid-cols-3 gap-4">
          <Card className="lg:col-span-1">
            {completion.isLoading && <Loading />}
            {completion.isError && <ErrorBox error={completion.error} />}
            {completion.data && (
              <div className="space-y-3">
                <Kpi
                  label="Link rate"
                  value={`${completion.data.link_rate_pct.toFixed(1)}%`}
                  hint={`${full(completion.data.matched)} of ${full(completion.data.bridge_outs)} bridge_outs matched`}
                />
                <Kpi
                  label="Unmatched"
                  value={full(completion.data.unmatched)}
                  hint="bridge_out without a bridge_in within 7d"
                />
              </div>
            )}
          </Card>
          <Card className="lg:col-span-2">
            {crossChain.isLoading && <Loading />}
            {crossChain.isError && <ErrorBox error={crossChain.error} />}
            {crossChain.data && (
              <Table
                data={crossChain.data.rows}
                columns={[
                  { key: 'route', label: 'src → dst', render: (r) => `${r.src_chain} → ${r.dst_chain}` },
                  { key: 'bridges', label: 'Bridges', align: 'right', render: (r) => full(r.bridges) },
                  { key: 'wallets', label: 'Wallets', align: 'right', render: (r) => full(r.wallets) },
                  {
                    key: 'avg_latency_seconds',
                    label: 'Avg latency',
                    align: 'right',
                    render: (r) => durationFromSeconds(r.avg_latency_seconds),
                  },
                  {
                    key: 'p50_latency_seconds',
                    label: 'p50 latency',
                    align: 'right',
                    render: (r) => durationFromSeconds(r.p50_latency_seconds),
                  },
                ]}
              />
            )}
          </Card>
        </div>
      </Section>

      <Section title="Post-bridge action split" subtitle="What did users do within 24h of bridging in?">
        <div className="grid lg:grid-cols-2 gap-4">
          <Card>
            <h3 className="text-sm font-medium mb-2">Swap vs non-swap vs no-action</h3>
            <div className="h-64">
              {split.isLoading ? (
                <Loading />
              ) : (
                <ResponsiveContainer>
                  <BarChart data={split.data?.rows ?? []}>
                    <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                    <XAxis dataKey="bucket" stroke="var(--color-muted)" />
                    <YAxis stroke="var(--color-muted)" tickFormatter={compact} />
                    <Tooltip
                      contentStyle={{
                        background: 'var(--color-panel)',
                        border: '1px solid var(--color-border)',
                      }}
                      formatter={(v) => full(Number(v))}
                    />
                    <Bar dataKey="n" radius={[4, 4, 0, 0]}>
                      {(split.data?.rows ?? []).map((r, i) => (
                        <Cell
                          key={i}
                          fill={
                            r.bucket === 'swap'
                              ? BAR_FILL
                              : r.bucket === 'other_defi'
                                ? BAR_ALT_FILL
                                : NO_ACTION_FILL
                          }
                        />
                      ))}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              )}
            </div>
          </Card>

          <Card>
            <h3 className="text-sm font-medium mb-2">Activity by hour after bridge (0–24h)</h3>
            <div className="h-64">
              {activity.isLoading ? (
                <Loading />
              ) : (
                <ResponsiveContainer>
                  <LineChart data={activity.data?.rows ?? []}>
                    <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                    <XAxis
                      dataKey="hour_offset"
                      stroke="var(--color-muted)"
                      label={{ value: 'hours after bridge', position: 'insideBottom', offset: -2, fill: 'var(--color-muted)' }}
                    />
                    <YAxis stroke="var(--color-muted)" tickFormatter={compact} />
                    <Tooltip
                      contentStyle={{
                        background: 'var(--color-panel)',
                        border: '1px solid var(--color-border)',
                      }}
                      formatter={(v) => full(Number(v))}
                    />
                    <Line
                      type="monotone"
                      dataKey="n"
                      stroke={BAR_FILL}
                      strokeWidth={2}
                      dot={false}
                    />
                  </LineChart>
                </ResponsiveContainer>
              )}
            </div>
          </Card>
        </div>
      </Section>

      <Section title="First action per bridge protocol" subtitle="Median time to first meaningful DeFi action.">
        <Card>
          {firstAction.isLoading ? (
            <Loading />
          ) : firstAction.isError ? (
            <ErrorBox error={firstAction.error} />
          ) : (
            <Table
              data={firstAction.data?.rows ?? []}
              columns={[
                { key: 'bridge_protocol', label: 'Bridge' },
                { key: 'next_app', label: 'Next app' },
                { key: 'action_type', label: 'Action' },
                { key: 'n', label: 'Users', align: 'right', render: (r) => full(r.n) },
                {
                  key: 'median_seconds',
                  label: 'Median time',
                  align: 'right',
                  render: (r) => durationFromSeconds(r.median_seconds),
                },
              ]}
            />
          )}
        </Card>
      </Section>

      <Section title="Top protocols used within 24h of bridging">
        <Card>
          <div className="h-72">
            {topProtocols.isLoading ? (
              <Loading />
            ) : (
              <ResponsiveContainer>
                <BarChart
                  data={topProtocols.data?.rows ?? []}
                  layout="vertical"
                  margin={{ left: 60 }}
                >
                  <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                  <XAxis type="number" stroke="var(--color-muted)" tickFormatter={compact} />
                  <YAxis type="category" dataKey="protocol" stroke="var(--color-muted)" width={100} />
                  <Tooltip
                    contentStyle={{
                      background: 'var(--color-panel)',
                      border: '1px solid var(--color-border)',
                    }}
                    formatter={(v) => full(Number(v))}
                  />
                  <Bar dataKey="n" fill={BAR_FILL} radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </Card>
      </Section>

      <Section title="2nd hop after the first post-bridge swap">
        <Card>
          {secondHop.isLoading ? (
            <Loading />
          ) : (
            <Table
              data={secondHop.data?.rows.slice(0, 20) ?? []}
              columns={[
                { key: 'after_swap_on', label: 'Swap was on' },
                { key: 'next_action', label: 'Then they…' },
                { key: 'next_protocol', label: 'on protocol' },
                { key: 'n', label: 'Users', align: 'right', render: (r) => full(r.n) },
              ]}
              empty="No second-hop activity in this window."
            />
          )}
        </Card>
      </Section>

      <Section title="Bridge events by protocol + chain">
        <Card>
          {breakdown.isLoading ? (
            <Loading />
          ) : (
            <Table
              data={breakdown.data?.rows ?? []}
              columns={[
                { key: 'chain', label: 'Chain' },
                { key: 'bridge_protocol', label: 'Bridge' },
                { key: 'bridge_ins', label: 'Ins', align: 'right', render: (r) => full(r.bridge_ins) },
                { key: 'bridge_outs', label: 'Outs', align: 'right', render: (r) => full(r.bridge_outs) },
                { key: 'total', label: 'Total', align: 'right', render: (r) => full(r.total) },
              ]}
            />
          )}
        </Card>
      </Section>

      <Section title="Method">
        <Banner>
          Swap share = swaps / (swaps + non-swap DeFi). The "non-swap DeFi" counter only fires
          when lending / staking / LP decoders are wired up — until then it's near zero by
          design, not by bug. Window: {days} days · Chain: {chain === 'all' ? 'all 5 chains' : chain}.
        </Banner>
      </Section>
    </div>
  );
}
