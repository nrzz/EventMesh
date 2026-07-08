import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { Card, ErrorState, LoadingState, PageHeader, StatusPill } from '../components/ui';

export function PluginsPage() {
  const { data, loading, error, reload } = useFetch(() => api.getPlugins(), []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader title="Plugins" description="Installed and discovered EventMesh plugins" />

      <div className="grid gap-4 sm:grid-cols-2">
        {(data ?? []).map((plugin) => (
          <Card key={plugin.name}>
            <div className="flex items-start justify-between">
              <div>
                <h3 className="font-medium text-white">{plugin.name}</h3>
                <p className="font-mono text-xs text-slate-500">v{plugin.version}</p>
              </div>
              <StatusPill status={plugin.enabled ? 'healthy' : 'degraded'} />
            </div>
            {plugin.description && (
              <p className="mt-3 text-sm text-slate-400">{plugin.description}</p>
            )}
            <div className="mt-4 flex flex-wrap gap-2">
              {plugin.tags.map((tag) => (
                <span
                  key={tag}
                  className="rounded-full bg-slate-800 px-2 py-0.5 text-xs text-slate-400"
                >
                  {tag}
                </span>
              ))}
            </div>
            <p className="mt-3 text-xs text-slate-500">
              {plugin.author ? `By ${plugin.author} · ` : ''}
              Status: {plugin.status}
            </p>
          </Card>
        ))}
      </div>
    </div>
  );
}
