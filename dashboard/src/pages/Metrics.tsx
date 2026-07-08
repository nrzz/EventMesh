import { api } from '../services/api';
import { useSignalRContext } from '../context/SignalRContext';
import { useFetch } from '../hooks/useFetch';
import { Card, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function MetricsPage() {
  const signalR = useSignalRContext();
  const { data, loading, error, reload } = useFetch(() => api.getMetrics(), []);
  const snapshot = signalR.metrics ?? data;

  if (loading && !snapshot) return <LoadingState />;
  if (error && !snapshot) return <ErrorState message={error} onRetry={reload} />;
  if (!snapshot) return null;

  const grouped = snapshot.metrics.reduce<Record<string, typeof snapshot.metrics>>((acc, metric) => {
    const key = metric.name.split('.').slice(0, 3).join('.');
    acc[key] ??= [];
    acc[key].push(metric);
    return acc;
  }, {});

  return (
    <div>
      <PageHeader
        title="Metrics"
        description="EventMesh operational metrics and Prometheus counters"
        actions={
          <a
            href="/metrics"
            target="_blank"
            rel="noreferrer"
            className="rounded-lg bg-slate-800 px-3 py-2 text-xs text-slate-300 hover:bg-slate-700"
          >
            Open Prometheus Endpoint
          </a>
        }
      />

      <p className="mb-6 text-sm text-slate-500">
        Captured at {new Date(snapshot.capturedAt).toLocaleString()}
      </p>

      <div className="grid gap-4">
        {Object.entries(grouped).map(([group, metrics]) => (
          <Card key={group}>
            <h3 className="font-mono text-sm text-mesh-400">{group}</h3>
            <div className="mt-3 divide-y divide-slate-800">
              {metrics.map((metric, index) => (
                <div key={`${metric.name}-${index}`} className="flex items-center justify-between py-2">
                  <div>
                    <p className="font-mono text-xs text-slate-300">{metric.name}</p>
                    {metric.description && (
                      <p className="text-xs text-slate-500">{metric.description}</p>
                    )}
                  </div>
                  <div className="text-right">
                    <p className="font-mono text-sm text-white">
                      {metric.value.toLocaleString()}
                      {metric.unit ? ` ${metric.unit}` : ''}
                    </p>
                    <p className="text-xs text-slate-500">{metric.type}</p>
                  </div>
                </div>
              ))}
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}
