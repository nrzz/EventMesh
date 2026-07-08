import { useState } from 'react';
import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function TopicsPage() {
  const [search, setSearch] = useState('');
  const { data, loading, error, reload } = useFetch(
    () => api.getTopics(1, 100, search || undefined),
    [search],
  );

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader
        title="Topics"
        description="Messaging topics, exchanges, and streams"
        actions={
          <input
            type="search"
            placeholder="Search topics..."
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
          { key: 'type', header: 'Type' },
          { key: 'partitions', header: 'Partitions' },
          {
            key: 'messageCount',
            header: 'Messages',
            render: (row) => Number(row.messageCount).toLocaleString(),
          },
          {
            key: 'publishRate',
            header: 'Publish Rate',
            render: (row) => `${Number(row.publishRate).toFixed(1)}/s`,
          },
        ]}
        rows={(data?.items ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
