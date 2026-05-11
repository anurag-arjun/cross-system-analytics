import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom';

import { Layout } from '@/components/Layout';
import { BridgeFlowPage } from '@/pages/BridgeFlow';
import { SpikesPage } from '@/pages/Spikes';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      // Data refreshes on the hour via cron — no need for aggressive client polling.
      staleTime: 60_000,
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route path="/" element={<Navigate to="/bridge" replace />} />
            <Route path="/bridge" element={<BridgeFlowPage />} />
            <Route path="/spikes" element={<SpikesPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
