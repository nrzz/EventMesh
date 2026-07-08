import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function RetriesPage() {
  const { data, loading, error, reload } = useFetch(() => api.getRetries(), []);

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader title="Retries" description="Pending message retry attempts" />

      <DataTable
        columns={[
          { key: 'messageId', header: 'Message ID', render: (row) => String(row.messageId).slice(0, 12) + '…' },
          { key: 'destination', header: 'Destination' },
          {
            key: 'attempt',
            header: 'Attempt',
            render: (row) => `${row.attempt} / ${row.maxAttempts}`,
          },
          {
            key: 'nextRetryAt',
            header: 'Next Retry',
            render: (row) =>
              row.nextRetryAt ? new Date(String(row.nextRetryAt)).toLocaleString() : '—',
          },
          { key: 'failureReason', header: 'Failure Reason' },
        ]}
        rows={(data?.items ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
