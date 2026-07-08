import { useState } from 'react';
import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function QueuesPage() {
  const [search, setSearch] = useState('');
  const { data, loading, error, reload } = useFetch(
    () => api.getQueues(1, 100, search || undefined),
    [search],
  );

  const handlePurge = async (name: string) => {
    if (!confirm(`Purge all messages from queue "${name}"?`)) return;
    await api.purgeQueue(name);
    await reload();
  };

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader
        title="Queues"
        description="Queue depths, in-flight messages, and consumer counts"
        actions={
          <input
            type="search"
            placeholder="Search queues..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:border-mesh-500 focus:outline-none"
          />
        }
      />

      <DataTable
        columns={[
          { key: 'name', header: 'Name' },
          { key: 'transport', header: 'Transport' },
          {
            key: 'depth',
            header: 'Depth',
            render: (row) => Number(row.depth).toLocaleString(),
          },
          { key: 'inFlight', header: 'In Flight' },
          { key: 'consumerCount', header: 'Consumers' },
          {
            key: 'durable',
            header: 'Durable',
            render: (row) => (row.durable ? 'Yes' : 'No'),
          },
          { key: 'deadLetterDestination', header: 'DLQ' },
          {
            key: 'actions',
            header: 'Actions',
            render: (row) => (
              <button
                type="button"
                onClick={() => void handlePurge(String(row.name))}
                className="rounded bg-rose-500/15 px-2 py-1 text-xs text-rose-300 hover:bg-rose-500/25"
              >
                Purge
              </button>
            ),
          },
        ]}
        rows={(data?.items ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
