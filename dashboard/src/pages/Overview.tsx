import { api } from '../services/api';
import { useSignalRContext } from '../context/SignalRContext';
import { useFetch } from '../hooks/useFetch';
import { Card, ErrorState, LoadingState, PageHeader, StatCard, StatusPill } from '../components/ui';

export function OverviewPage() {
  const signalR = useSignalRContext();
  const { data, loading, error, reload } = useFetch(() => api.getOverview(), []);

  const overview = signalR.overview ?? data;

  if (loading && !overview) return <LoadingState />;
  if (error && !overview) return <ErrorState message={error} onRetry={reload} />;
  if (!overview) return null;

  return (
    <div>
      <PageHeader
        title="Overview"
        description="Real-time summary of your EventMesh cluster"
        actions={<StatusPill status={overview.clusterStatus} />}
      />

      <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Connections"
          value={`${overview.healthyConnections}/${overview.connectionCount}`}
          subtext="Healthy connections"
          accent="emerald"
        />
        <StatCard label="Topics" value={overview.topicCount} subtext="Active topics" />
        <StatCard
          label="Queue Depth"
          value={overview.totalQueueDepth.toLocaleString()}
          subtext={`${overview.queueCount} queues`}
          accent="amber"
        />
        <StatCard
          label="Consumers"
          value={overview.activeConsumers}
          subtext="Active consumers"
        />
      </div>

      <div className="mt-4 grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
        <StatCard
          label="Publish Rate"
          value={`${overview.messagesPublishedPerSecond.toFixed(1)}/s`}
          subtext="Messages published"
        />
        <StatCard
          label="Consume Rate"
          value={`${overview.messagesConsumedPerSecond.toFixed(1)}/s`}
          subtext="Messages consumed"
        />
        <StatCard
          label="Pending Retries"
          value={overview.pendingRetries}
          accent="amber"
        />
        <StatCard
          label="Dead Letters"
          value={overview.deadLetterCount}
          accent="rose"
        />
      </div>

      <Card className="mt-6">
        <h3 className="text-sm font-medium text-slate-300">Cluster Activity</h3>
        <p className="mt-2 text-sm text-slate-500">
          Snapshot generated at {new Date(overview.generatedAt).toLocaleString()}.
          {signalR.connected
            ? ' Receiving live updates via SignalR.'
            : ' SignalR disconnected — showing last REST snapshot.'}
        </p>
      </Card>
    </div>
  );
}
