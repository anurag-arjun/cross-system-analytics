/**
 * Spike Detection page — matches section 2 of the BD requirements doc.
 *
 * Layout:
 *   row 1: KPI cards (venues, extreme, high)
 *   row 2: methodology banner
 *   row 3: hourly extreme + high tables
 *   row 4: daily extreme + high tables
 *   row 5: hourly timeline line chart
 *   row 6: active-protocols bar
 */

import { useQuery } from '@tanstack/react-query';
import { useState } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
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
  fetchActiveProtocols,
  fetchDailySpikes,
  fetchHourlySpikes,
  fetchSpikeSummary,
  fetchTimeline,
  type Chain,
  type SpikeRow,
} from '@/lib/api';
import { compact, full, ratio, shortAddr } from '@/lib/format';

const ACCENT = 'oklch(0.72 0.18 250)';

export function SpikesPage() {
  const [chain, setChain] = useState<Chain>('all');
  const [days, setDays] = useState<number>(7);

  const summary = useQuery({
    queryKey: ['spike-summary', days],
    queryFn: () => fetchSpikeSummary(days),
  });
  const hourlyExtreme = useQuery({
    queryKey: ['hourly-spikes', days, chain, 'extreme'],
    queryFn: () => fetchHourlySpikes(days, chain, 'extreme', 25),
  });
  const hourlyHigh = useQuery({
    queryKey: ['hourly-spikes', days, chain, 'high'],
    queryFn: () => fetchHourlySpikes(days, chain, 'high', 25),
  });
  const dailyExtreme = useQuery({
    queryKey: ['daily-spikes', days, chain, 'extreme'],
    queryFn: () => fetchDailySpikes(days, chain, 'extreme', 25),
  });
  const dailyHigh = useQuery({
    queryKey: ['daily-spikes', days, chain, 'high'],
    queryFn: () => fetchDailySpikes(days, chain, 'high', 25),
  });
  const timeline = useQuery({
    queryKey: ['timeline', days, chain],
    queryFn: () => fetchTimeline(days, chain),
  });
  const protocols = useQuery({
    queryKey: ['active-protocols', days, chain],
    queryFn: () => fetchActiveProtocols(days, chain, 20),
  });

  return (
    <div>
      <header className="flex flex-col gap-3">
        <h1 className="text-2xl font-bold">Spike Detection</h1>
        <p className="text-sm text-[var(--color-muted)] max-w-3xl">
          Apps/contracts with abnormal activity vs their own rolling baseline. Two
          timescales — hourly (vs 24h rolling avg) catches intraday surges; daily (vs 7d
          rolling avg) catches slower trends.
        </p>
        <Filters chain={chain} setChain={setChain} days={days} setDays={setDays} />
      </header>

      {summary.isError && <ErrorBox error={summary.error} />}

      <div className="grid grid-cols-2 md:grid-cols-3 gap-3 mt-6">
        <Kpi
          label="Venues tracked"
          value={summary.data ? compact(summary.data.venues_tracked) : '—'}
          hint="hourly (venue, protocol, chain) combinations"
        />
        <Kpi
          label="Extreme spikes"
          value={summary.data ? full(summary.data.extreme_alerts) : '—'}
          hint="≥ 400% vs rolling avg"
          accent="extreme"
        />
        <Kpi
          label="High spikes"
          value={summary.data ? full(summary.data.high_alerts) : '—'}
          hint="≥ 200% vs rolling avg"
          accent="high"
        />
      </div>

      <Section
        title="Hourly extreme spikes (≥400%)"
        subtitle="Hour-bucketed activity vs the venue's own 24h rolling average."
      >
        <Card>
          {hourlyExtreme.isLoading ? <Loading /> : <SpikeTable rows={hourlyExtreme.data?.rows ?? []} timeKey="hour" />}
        </Card>
      </Section>

      <Section title="Hourly high spikes (≥200%)">
        <Card>
          {hourlyHigh.isLoading ? <Loading /> : <SpikeTable rows={hourlyHigh.data?.rows ?? []} timeKey="hour" />}
        </Card>
      </Section>

      <Section
        title="Daily extreme spikes (≥400% vs 7d avg)"
        subtitle="Day-bucketed activity vs the venue's own 7-day rolling average."
      >
        <Card>
          {dailyExtreme.isLoading ? <Loading /> : <SpikeTable rows={dailyExtreme.data?.rows ?? []} timeKey="day" />}
        </Card>
      </Section>

      <Section title="Daily high spikes (≥200% vs 7d avg)">
        <Card>
          {dailyHigh.isLoading ? <Loading /> : <SpikeTable rows={dailyHigh.data?.rows ?? []} timeKey="day" />}
        </Card>
      </Section>

      <Section title="Hourly activity timeline">
        <Card>
          <div className="h-64">
            {timeline.isLoading ? (
              <Loading />
            ) : (
              <ResponsiveContainer>
                <LineChart data={timeline.data?.rows ?? []}>
                  <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                  <XAxis dataKey="hour" stroke="var(--color-muted)" hide />
                  <YAxis stroke="var(--color-muted)" tickFormatter={compact} />
                  <Tooltip
                    contentStyle={{
                      background: 'var(--color-panel)',
                      border: '1px solid var(--color-border)',
                    }}
                    formatter={(v) => full(Number(v))}
                  />
                  <Line type="monotone" dataKey="events" stroke={ACCENT} strokeWidth={2} dot={false} />
                </LineChart>
              </ResponsiveContainer>
            )}
          </div>
        </Card>
      </Section>

      <Section title="Active protocols by venue count">
        <Card>
          <div className="h-72">
            {protocols.isLoading ? (
              <Loading />
            ) : (
              <ResponsiveContainer>
                <BarChart
                  data={protocols.data?.rows ?? []}
                  layout="vertical"
                  margin={{ left: 60 }}
                >
                  <CartesianGrid stroke="var(--color-border)" strokeDasharray="3 3" />
                  <XAxis type="number" stroke="var(--color-muted)" tickFormatter={compact} />
                  <YAxis type="category" dataKey="protocol" stroke="var(--color-muted)" width={140} />
                  <Tooltip
                    contentStyle={{
                      background: 'var(--color-panel)',
                      border: '1px solid var(--color-border)',
                    }}
                    formatter={(v) => full(Number(v))}
                  />
                  <Bar dataKey="venues" fill={ACCENT} radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </Card>
      </Section>

      <Section title="Method">
        <Banner>
          Hourly threshold: ≥3 events/hour, &gt;0 rolling avg. Daily threshold: ≥10
          events/day with ≥3 prior days of history and rolling avg ≥5 events — keeps the
          early-coverage window from flagging everything as a spike. Window: {days} days · Chain:{' '}
          {chain === 'all' ? 'all 5 chains' : chain}.
        </Banner>
      </Section>
    </div>
  );
}

function SpikeTable({ rows, timeKey }: { rows: SpikeRow[]; timeKey: 'hour' | 'day' }) {
  return (
    <Table
      data={rows}
      columns={[
        {
          key: 'venue',
          label: 'Venue',
          render: (r) => (
            <code className="text-[11px]" title={r.venue}>
              {shortAddr(r.venue.replace('…', ''))}
            </code>
          ),
        },
        { key: 'protocol', label: 'Protocol' },
        { key: 'chain', label: 'Chain' },
        {
          key: timeKey,
          label: timeKey === 'hour' ? 'Hour' : 'Day',
          render: (r) => <span className="text-xs text-[var(--color-muted)]">{r[timeKey]}</span>,
        },
        { key: 'events', label: 'Events', align: 'right', render: (r) => full(r.events) },
        { key: 'wallets', label: 'Wallets', align: 'right', render: (r) => full(r.wallets) },
        {
          key: 'events_ratio',
          label: 'Events',
          align: 'right',
          render: (r) => <span className="text-[var(--color-alert-extreme)]">{ratio(r.events_ratio)}</span>,
        },
        {
          key: 'wallets_ratio',
          label: 'Wallets',
          align: 'right',
          render: (r) => <span className="text-[var(--color-alert-high)]">{ratio(r.wallets_ratio)}</span>,
        },
      ]}
      empty="No spikes in the current window."
    />
  );
}
