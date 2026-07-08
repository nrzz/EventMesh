import type {
  ClusterHealthInfo,
  ConnectionInfo,
  ConsumerInfo,
  DeadLetterInfo,
  MetricsSnapshot,
  OverviewInfo,
  PagedResult,
  PluginInfo,
  QueueInfo,
  ReplayJobInfo,
  ReplayRequest,
  RetryInfo,
  TopicInfo,
  MessageInfo,
  DashboardSettings,
} from '../types/api';

const SETTINGS_KEY = 'eventmesh.dashboard.settings';

const defaultSettings: DashboardSettings = {
  apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
  apiKey: import.meta.env.VITE_API_KEY ?? '',
  refreshIntervalMs: 5000,
};

function getSettings(): DashboardSettings {
  try {
    const stored = localStorage.getItem(SETTINGS_KEY);
    if (!stored) {
      return defaultSettings;
    }
    return { ...defaultSettings, ...JSON.parse(stored) };
  } catch {
    return defaultSettings;
  }
}

export function saveSettings(settings: DashboardSettings): void {
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
}

export function getDashboardSettings(): DashboardSettings {
  return getSettings();
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const settings = getSettings();
  const headers = new Headers(init?.headers);
  headers.set('Accept', 'application/json');

  if (!headers.has('Content-Type') && init?.body) {
    headers.set('Content-Type', 'application/json');
  }

  if (settings.apiKey) {
    headers.set('X-Api-Key', settings.apiKey);
  }

  const response = await fetch(`${settings.apiBaseUrl}${path}`, {
    ...init,
    headers,
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `Request failed with status ${response.status}`);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}

export const api = {
  getOverview: () => request<OverviewInfo>('/api/connections/overview'),
  getConnections: () => request<ConnectionInfo[]>('/api/connections'),
  getTopics: (page = 1, pageSize = 50, search?: string) => {
    const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
    if (search) params.set('search', search);
    return request<PagedResult<TopicInfo>>(`/api/topics?${params}`);
  },
  getQueues: (page = 1, pageSize = 50, search?: string) => {
    const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
    if (search) params.set('search', search);
    return request<PagedResult<QueueInfo>>(`/api/queues?${params}`);
  },
  purgeQueue: (name: string) =>
    request<void>(`/api/queues/${encodeURIComponent(name)}/purge`, { method: 'POST' }),
  getMessages: (page = 1, pageSize = 50, source?: string, type?: string) => {
    const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
    if (source) params.set('source', source);
    if (type) params.set('type', type);
    return request<PagedResult<MessageInfo>>(`/api/messages?${params}`);
  },
  getConsumers: () => request<ConsumerInfo[]>('/api/consumers'),
  getRetries: (page = 1, pageSize = 50) => {
    const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
    return request<PagedResult<RetryInfo>>(`/api/retries?${params}`);
  },
  getDeadLetters: (page = 1, pageSize = 50) => {
    const params = new URLSearchParams({ page: String(page), pageSize: String(pageSize) });
    return request<PagedResult<DeadLetterInfo>>(`/api/deadletters?${params}`);
  },
  reprocessDeadLetter: (id: string, destination?: string) =>
    request<void>(`/api/deadletters/${encodeURIComponent(id)}/reprocess`, {
      method: 'POST',
      body: JSON.stringify({ destination }),
    }),
  getReplayJobs: () => request<ReplayJobInfo[]>('/api/replay'),
  startReplay: (payload: ReplayRequest) =>
    request<ReplayJobInfo>('/api/replay', {
      method: 'POST',
      body: JSON.stringify(payload),
    }),
  getMetrics: () => request<MetricsSnapshot>('/api/metrics'),
  getPlugins: () => request<PluginInfo[]>('/api/plugins'),
  getClusterHealth: () => request<ClusterHealthInfo>('/api/health'),
};
