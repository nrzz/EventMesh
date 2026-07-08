import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader, StatusPill } from '../components/ui';

export function ConsumersPage() {
  const { data, loading, error, reload } = useFetch(() => api.getConsumers(), []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader title="Consumers" description="Active message consumers and handlers" />

      <DataTable
        columns={[
          { key: 'name', header: 'Name' },
          { key: 'destination', header: 'Destination' },
          { key: 'transport', header: 'Transport' },
          {
            key: 'status',
            header: 'Status',
            render: (row) => <StatusPill status={String(row.status)} />,
          },
          { key: 'concurrency', header: 'Concurrency' },
          {
            key: 'messagesProcessed',
            header: 'Processed',
            render: (row) => Number(row.messagesProcessed).toLocaleString(),
          },
          {
            key: 'lastMessageAt',
            header: 'Last Message',
            render: (row) =>
              row.lastMessageAt
                ? new Date(String(row.lastMessageAt)).toLocaleString()
                : '—',
          },
        ]}
        rows={(data ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
