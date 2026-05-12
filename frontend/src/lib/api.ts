/**
 * Thin typed client for the Nexus BD API.
 *
 * Endpoint surface mirrors api/main.py — see api/README.md for params.
 * Dev: the Vite proxy forwards /api/* to FastAPI on :8000. Production:
 * the static bundle and the API are served from the same origin so the
 * paths resolve naturally without proxying.
 */

export type Chain = 'all' | 'ethereum' | 'base' | 'arbitrum' | 'optimism' | 'polygon';
export type Alert = 'all' | 'extreme' | 'high';

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? '';

async function get<T>(path: string, params: Record<string, string | number | undefined>): Promise<T> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== null && v !== '') qs.set(k, String(v));
  }
  const url = `${BASE}${path}${qs.toString() ? `?${qs.toString()}` : ''}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} — ${url}`);
  return (await res.json()) as T;
}

// ----------------- Bridge flow -----------------

export interface BridgeSummary {
  bridge_ins: number;
  bridge_outs: number;
  swaps: number;
  non_swap_defi: number;
  swap_share: number;
}

export const fetchBridgeSummary = (days: number, chain: Chain) =>
  get<BridgeSummary>('/api/bridge-flow/summary', { days, chain });

export interface BridgeBreakdownRow {
  chain: string;
  bridge_protocol: string;
  bridge_ins: number;
  bridge_outs: number;
  total: number;
}
export const fetchBridgeBreakdown = (days: number, chain: Chain) =>
  get<{ rows: BridgeBreakdownRow[] }>('/api/bridge-flow/breakdown', { days, chain });

export interface FirstActionRow {
  bridge_protocol: string;
  next_app: string;
  action_type: string;
  n: number;
  median_seconds: number;
}
export const fetchFirstAction = (days: number, chain: Chain) =>
  get<{ rows: FirstActionRow[] }>('/api/bridge-flow/first-action', { days, chain });

export interface SwapVsNonSwapRow {
  bucket: 'swap' | 'other_defi' | 'no_action';
  n: number;
}
export const fetchSwapVsNonSwap = (days: number, chain: Chain) =>
  get<{ rows: SwapVsNonSwapRow[] }>('/api/bridge-flow/swap-vs-non-swap', { days, chain });

export interface SecondHopRow {
  after_swap_on: string;
  next_action: string;
  next_protocol: string;
  n: number;
}
export const fetchSecondHop = (days: number, chain: Chain) =>
  get<{ rows: SecondHopRow[] }>('/api/bridge-flow/second-hop', { days, chain });

export interface Activity24hRow {
  hour_offset: number;
  n: number;
}
export const fetchActivity24h = (days: number, chain: Chain) =>
  get<{ rows: Activity24hRow[] }>('/api/bridge-flow/activity-24h', { days, chain });

export interface TopProtocolRow {
  protocol: string;
  n: number;
}
export const fetchTopProtocols = (days: number, chain: Chain) =>
  get<{ rows: TopProtocolRow[] }>('/api/bridge-flow/top-protocols-after-bridge', { days, chain });

// Cross-chain (powered by the materialised bridge_links JOIN).
export interface CrossChainRow {
  src_chain: string;
  dst_chain: string;
  bridges: number;
  wallets: number;
  total_usd: number;
  avg_latency_seconds: number;
  p50_latency_seconds: number;
}
export const fetchCrossChainMatrix = (days: number, chain: Chain) =>
  get<{ rows: CrossChainRow[] }>('/api/bridge-flow/cross-chain-matrix', { days, chain });

export interface BridgeCompletion {
  bridge_outs: number;
  matched: number;
  unmatched: number;
  link_rate_pct: number;
}
export const fetchBridgeCompletion = (days: number, chain: Chain) =>
  get<BridgeCompletion>('/api/bridge-flow/completion', { days, chain });

// ----------------- Spikes -----------------

export interface SpikeSummary {
  venues_tracked: number;
  extreme_alerts: number;
  high_alerts: number;
}
export const fetchSpikeSummary = (days: number) =>
  get<SpikeSummary>('/api/spikes/summary', { days });

export interface SpikeRow {
  venue: string;
  protocol: string;
  chain: string;
  hour?: string;
  day?: string;
  events: number;
  wallets: number;
  events_ratio: number;
  wallets_ratio: number;
  alert: 'extreme' | 'high' | 'normal';
}
export const fetchHourlySpikes = (days: number, chain: Chain, alert: Alert, limit = 50) =>
  get<{ rows: SpikeRow[] }>('/api/spikes/hourly', { days, chain, alert, limit });
export const fetchDailySpikes = (days: number, chain: Chain, alert: Alert, limit = 50) =>
  get<{ rows: SpikeRow[] }>('/api/spikes/daily', { days, chain, alert, limit });

export interface TimelinePoint {
  hour: string;
  events: number;
  wallets: number;
}
export const fetchTimeline = (days: number, chain: Chain) =>
  get<{ rows: TimelinePoint[] }>('/api/spikes/timeline', { days, chain });

export interface ActiveProtocolRow {
  protocol: string;
  chain: string;
  venues: number;
  events: number;
  wallets: number;
}
export const fetchActiveProtocols = (days: number, chain: Chain, limit = 25) =>
  get<{ rows: ActiveProtocolRow[] }>('/api/spikes/protocols', { days, chain, limit });
