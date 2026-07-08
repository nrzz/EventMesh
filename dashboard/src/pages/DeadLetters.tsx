import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { DataTable, ErrorState, LoadingState, PageHeader } from '../components/ui';

export function DeadLettersPage() {
  const { data, loading, error, reload } = useFetch(() => api.getDeadLetters(), []);

  const handleReprocess = async (id: string) => {
    await api.reprocessDeadLetter(id);
    await reload();
  };

  if (loading) return <LoadingState />;
  if (error) return <ErrorState message={error} onRetry={reload} />;

  return (
    <div>
      <PageHeader title="Dead Letters" description="Failed messages in dead-letter queues" />

      <DataTable
        columns={[
          { key: 'type', header: 'Type' },
          { key: 'originalDestination', header: 'Original' },
          { key: 'deadLetterDestination', header: 'DLQ' },
          { key: 'deliveryAttempts', header: 'Attempts' },
          { key: 'failureReason', header: 'Reason' },
          {
            key: 'deadLetteredAt',
            header: 'Dead Lettered',
            render: (row) => new Date(String(row.deadLetteredAt)).toLocaleString(),
          },
          {
            key: 'actions',
            header: 'Actions',
            render: (row) => (
              <button
                type="button"
                onClick={() => void handleReprocess(String(row.id))}
                className="rounded bg-mesh-500/15 px-2 py-1 text-xs text-mesh-300 hover:bg-mesh-500/25"
              >
                Reprocess
              </button>
            ),
          },
        ]}
        rows={(data?.items ?? []) as unknown as Record<string, unknown>[]}
      />
    </div>
  );
}
