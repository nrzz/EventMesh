import { useState } from 'react';
import { api } from '../services/api';
import { useFetch } from '../hooks/useFetch';
import { Card, DataTable, ErrorState, LoadingState, PageHeader, StatusPill } from '../components/ui';

export function ReplayPage() {
  const [source, setSource] = useState('orders.created');
  const [destination, setDestination] = useState('');
  const [maxMessages, setMaxMessages] = useState(100);
  const [submitting, setSubmitting] = useState(false);

  const { data, loading, error, reload } = useFetch(() => api.getReplayJobs(), []);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setSubmitting(true);
    try {
      await api.startReplay({
        source,
        destination: destination || undefined,
        maxMessages,
      });
      await reload();
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div>
      <PageHeader title="Replay" description="Replay messages from topics and archives" />

      <Card className="mb-6">
        <h3 className="text-sm font-medium text-slate-300">Start Replay Job</h3>
        <form onSubmit={(e) => void handleSubmit(e)} className="mt-4 grid gap-4 sm:grid-cols-2">
          <label className="block text-sm">
            <span className="text-slate-400">Source</span>
            <input
              required
              value={source}
              onChange={(e) => setSource(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>
          <label className="block text-sm">
            <span className="text-slate-400">Destination (optional)</span>
            <input
              value={destination}
              onChange={(e) => setDestination(e.target.value)}
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>
          <label className="block text-sm">
            <span className="text-slate-400">Max Messages</span>
            <input
              type="number"
              min={1}
              value={maxMessages}
              onChange={(e) => setMaxMessages(Number(e.target.value))}
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>
          <div className="flex items-end">
            <button
              type="submit"
              disabled={submitting}
              className="rounded-lg bg-mesh-600 px-4 py-2 text-sm font-medium text-white hover:bg-mesh-500 disabled:opacity-50"
            >
              {submitting ? 'Starting…' : 'Start Replay'}
            </button>
          </div>
        </form>
      </Card>

      {loading ? (
        <LoadingState />
      ) : error ? (
        <ErrorState message={error} onRetry={reload} />
      ) : (
        <DataTable
          columns={[
            { key: 'id', header: 'Job ID', render: (row) => String(row.id).slice(0, 12) + '…' },
            { key: 'source', header: 'Source' },
            { key: 'destination', header: 'Destination' },
            {
              key: 'status',
              header: 'Status',
              render: (row) => <StatusPill status={String(row.status)} />,
            },
            {
              key: 'progress',
              header: 'Progress',
              render: (row) =>
                `${row.messagesReplayed}${row.totalMessages ? ` / ${row.totalMessages}` : ''}`,
            },
            {
              key: 'createdAt',
              header: 'Created',
              render: (row) => new Date(String(row.createdAt)).toLocaleString(),
            },
          ]}
          rows={(data ?? []) as unknown as Record<string, unknown>[]}
        />
      )}
    </div>
  );
}
