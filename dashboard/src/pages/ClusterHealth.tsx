import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { Card, ErrorState, LoadingState, PageHeader, StatusPill } from '../components/ui';

export function ClusterHealthPage() {
  const { data, loading, error, reload } = useFetch(() => api.getClusterHealth(), []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;
  if (!data) return null;

  return (
    <div>
      <PageHeader
        title="Cluster Health"
        description="Aggregated health of all mesh components"
        actions={<StatusPill status={data.status} />}
      />

      <div className="mb-6 grid gap-4 sm:grid-cols-3">
        <Card>
          <p className="text-xs uppercase text-slate-500">Overall Status</p>
          <p className="mt-2 text-xl font-semibold capitalize text-white">{data.status}</p>
        </Card>
        <Card>
          <p className="text-xs uppercase text-slate-500">API Version</p>
          <p className="mt-2 font-mono text-xl text-mesh-400">{data.version}</p>
        </Card>
        <Card>
          <p className="text-xs uppercase text-slate-500">Last Checked</p>
          <p className="mt-2 text-sm text-slate-300">
            {new Date(data.checkedAt).toLocaleString()}
          </p>
        </Card>
      </div>

      <div className="grid gap-4 sm:grid-cols-2">
        {data.components.map((component) => (
          <Card key={component.name}>
            <div className="flex items-center justify-between">
              <h3 className="font-medium text-white">{component.name}</h3>
              <StatusPill status={component.status} />
            </div>
            {component.description && (
              <p className="mt-2 text-sm text-slate-400">{component.description}</p>
            )}
            {Object.keys(component.metadata).length > 0 && (
              <dl className="mt-3 space-y-1">
                {Object.entries(component.metadata).map(([key, value]) => (
                  <div key={key} className="flex justify-between text-xs">
                    <dt className="text-slate-500">{key}</dt>
                    <dd className="font-mono text-slate-400">{value || '—'}</dd>
                  </div>
                ))}
              </dl>
            )}
          </Card>
        ))}
      </div>
    </div>
  );
}
