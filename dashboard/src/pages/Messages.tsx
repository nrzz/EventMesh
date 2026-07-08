import { useState } from 'react';
import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { Card, DataTable, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function MessagesPage() {
  const [source, setSource] = useState('');
  const { data, loading, error, reload } = useFetch(
    () => api.getMessages(1, 50, source || undefined),
    [source],
  );

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader
        title="Messages"
        description="Inspect recent messages across the mesh"
        actions={
          <input
            type="search"
            placeholder="Filter by source..."
            value={source}
            onChange={(e) => setSource(e.target.value)}
            className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-2 text-sm text-slate-200 placeholder:text-slate-500 focus:border-mesh-500 focus:outline-none"
          />
        }
      />

      <DataTable
        columns={[
          { key: 'id', header: 'ID', render: (row) => String(row.id).slice(0, 12) + '…' },
          { key: 'type', header: 'Type' },
          { key: 'source', header: 'Source' },
          { key: 'transport', header: 'Transport' },
          { key: 'status', header: 'Status' },
          {
            key: 'timestamp',
            header: 'Timestamp',
            render: (row) => new Date(String(row.timestamp)).toLocaleString(),
          },
          { key: 'sizeBytes', header: 'Size', render: (row) => `${row.sizeBytes} B` },
        ]}
        rows={(data?.items ?? []) as unknown as Record<string, unknown>[]}
      />

      {data?.items[0]?.payloadPreview && (
        <Card className="mt-4">
          <h3 className="text-sm font-medium text-slate-300">Latest Payload Preview</h3>
          <pre className="mt-2 overflow-x-auto rounded-lg bg-slate-950 p-4 font-mono text-xs text-slate-400">
            {data.items[0].payloadPreview}
          </pre>
        </Card>
      )}
    </div>
  );
}
