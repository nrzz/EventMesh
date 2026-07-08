import { Card, PageHeader } from '../components/ui';

export function TracingPage() {
  return (
    <div>
      <PageHeader
        title="Tracing"
        description="Distributed trace exploration for EventMesh operations"
      />

      <Card>
        <h3 className="text-sm font-medium text-slate-300">OpenTelemetry Traces</h3>
        <p className="mt-2 text-sm text-slate-400">
          EventMesh instruments all publish and consume operations with the{' '}
          <code className="rounded bg-slate-800 px-1.5 py-0.5 font-mono text-xs text-mesh-300">
            EventMesh
          </code>{' '}
          activity source. Configure an OTLP collector endpoint in the management API to export
          traces to Jaeger, Tempo, or your observability backend.
        </p>

        <div className="mt-6 grid gap-4 sm:grid-cols-2">
          <div className="rounded-lg border border-slate-800 bg-slate-950 p-4">
            <p className="text-xs font-medium uppercase text-slate-500">Activity Source</p>
            <p className="mt-1 font-mono text-sm text-mesh-400">EventMesh</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950 p-4">
            <p className="text-xs font-medium uppercase text-slate-500">Span Kinds</p>
            <p className="mt-1 text-sm text-slate-300">publish, consume, retry, dead-letter</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950 p-4">
            <p className="text-xs font-medium uppercase text-slate-500">Correlation</p>
            <p className="mt-1 text-sm text-slate-300">W3C trace context propagation</p>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950 p-4">
            <p className="text-xs font-medium uppercase text-slate-500">Export</p>
            <p className="mt-1 text-sm text-slate-300">OTLP gRPC / HTTP</p>
          </div>
        </div>
      </Card>
    </div>
  );
}
