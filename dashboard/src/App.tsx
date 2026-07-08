import { Navigate, Route, Routes } from 'react-router-dom';
import { Layout } from './components/Layout';
import { OverviewPage } from './pages/Overview';
import { ConnectionsPage } from './pages/Connections';
import { TopicsPage } from './pages/Topics';
import { QueuesPage } from './pages/Queues';
import { MessagesPage } from './pages/Messages';
import { ConsumersPage } from './pages/Consumers';
import { RetriesPage } from './pages/Retries';
import { DeadLettersPage } from './pages/DeadLetters';
import { ReplayPage } from './pages/Replay';
import { MetricsPage } from './pages/Metrics';
import { TracingPage } from './pages/Tracing';
import { PluginsPage } from './pages/Plugins';
import { SettingsPage } from './pages/Settings';
import { ClusterHealthPage } from './pages/ClusterHealth';

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<OverviewPage />} />
        <Route path="connections" element={<ConnectionsPage />} />
        <Route path="topics" element={<TopicsPage />} />
        <Route path="queues" element={<QueuesPage />} />
        <Route path="messages" element={<MessagesPage />} />
        <Route path="consumers" element={<ConsumersPage />} />
        <Route path="retries" element={<RetriesPage />} />
        <Route path="dead-letters" element={<DeadLettersPage />} />
        <Route path="replay" element={<ReplayPage />} />
        <Route path="metrics" element={<MetricsPage />} />
        <Route path="tracing" element={<TracingPage />} />
        <Route path="plugins" element={<PluginsPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="cluster-health" element={<ClusterHealthPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  );
}
