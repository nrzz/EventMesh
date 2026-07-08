import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader, StatusPill } from '../components/ui';

export function ConnectionsPage() {
  const { data, loading, error, reload } = useFetch(() => api.getConnections(), []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader
        title="Connections"
        description="Transport, database, and cache connection health"
      />

      <DataTable
        columns={[
          { key: 'name', header: 'Name' },
          { key: 'type', header: 'Type' },
          { key: 'endpoint', header: 'Endpoint' },
          {
            key: 'status',
            header: 'Status',
            render: (row) => <StatusPill status={String(row.status)} />,
          },
          {
            key: 'latencyMs',
            header: 'Latency',
            render: (row) =>
              row.latencyMs != null ? `${Number(row.latencyMs).toFixed(1)} ms` : '—',
          },
          {
            key: 'lastCheckedAt',
            header: 'Last Checked',
            render: (row) => new Date(String(row.lastCheckedAt)).toLocaleString(),
          },
          { key: 'error', header: 'Error' },
        ]}
        rows={(data ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
