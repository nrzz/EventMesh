import { useState } from 'react';
import { getDashboardSettings, saveSettings } from '../services/api';
import type { DashboardSettings } from '../types/api';
import { Card, PageHeader } from '../components/ui';

export function SettingsPage() {
  const [settings, setSettings] = useState<DashboardSettings>(getDashboardSettings());
  const [saved, setSaved] = useState(false);

  const handleSave = (e: React.FormEvent) => {
    e.preventDefault();
    saveSettings(settings);
    setSaved(true);
    setTimeout(() => setSaved(false), 2000);
  };

  return (
    <div>
      <PageHeader title="Settings" description="Dashboard and API connection settings" />

      <Card className="max-w-xl">
        <form onSubmit={handleSave} className="space-y-4">
          <label className="block text-sm">
            <span className="text-slate-400">API Base URL</span>
            <input
              value={settings.apiBaseUrl}
              onChange={(e) => setSettings({ ...settings, apiBaseUrl: e.target.value })}
              placeholder="Leave empty for same-origin"
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>

          <label className="block text-sm">
            <span className="text-slate-400">API Key</span>
            <input
              type="password"
              value={settings.apiKey}
              onChange={(e) => setSettings({ ...settings, apiKey: e.target.value })}
              placeholder="X-Api-Key header value"
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>

          <label className="block text-sm">
            <span className="text-slate-400">Refresh Interval (ms)</span>
            <input
              type="number"
              min={1000}
              step={1000}
              value={settings.refreshIntervalMs}
              onChange={(e) =>
                setSettings({ ...settings, refreshIntervalMs: Number(e.target.value) })
              }
              className="mt-1 w-full rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm"
            />
          </label>

          <button
            type="submit"
            className="rounded-lg bg-mesh-600 px-4 py-2 text-sm font-medium text-white hover:bg-mesh-500"
          >
            Save Settings
          </button>

          {saved && <p className="text-sm text-emerald-400">Settings saved.</p>}
        </form>
      </Card>
    </div>
  );
}
