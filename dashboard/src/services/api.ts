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

/**
 * The settings that may be persisted in the browser. The API key is deliberately absent: it is a
 * credential, so it is never written to localStorage or sessionStorage.
 */
type PersistedSettings = Pick<DashboardSettings, 'apiBaseUrl' | 'refreshIntervalMs'>;

// Key entered on the Settings page. It lives in memory for the life of the page only, so it is gone
// after a reload; until one is entered the key baked in at build time (VITE_API_KEY) is used.
let inMemoryApiKey: string | undefined;

function getDefaultSettings(): DashboardSettings {
  return {
    apiBaseUrl: import.meta.env.VITE_API_BASE_URL ?? '',
    apiKey: import.meta.env.VITE_API_KEY ?? '',
    refreshIntervalMs: 5000,
  };
}

function readPersistedSettings(): Partial<PersistedSettings> {
  try {
    const stored = localStorage.getItem(SETTINGS_KEY);
    if (!stored) {
      return {};
    }

    const parsed = JSON.parse(stored) as Record<string, unknown>;
    const persisted: Partial<PersistedSettings> = {};
    if (typeof parsed.apiBaseUrl === 'string') {
      persisted.apiBaseUrl = parsed.apiBaseUrl;
    }
    if (typeof parsed.refreshIntervalMs === 'number') {
      persisted.refreshIntervalMs = parsed.refreshIntervalMs;
    }

    // Earlier builds persisted the whole settings object, API key included. Rewrite the entry
    // without it so a key saved by an old build does not linger in the browser.
    if (Object.prototype.hasOwnProperty.call(parsed, 'apiKey')) {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(persisted));
    }

    return persisted;
  } catch {
    return {};
  }
}

function getSettings(): DashboardSettings {
  const defaults = getDefaultSettings();
  return {
    ...defaults,
    ...readPersistedSettings(),
    apiKey: inMemoryApiKey ?? defaults.apiKey,
  };
}

export function saveSettings(settings: DashboardSettings): void {
  inMemoryApiKey = settings.apiKey;

  const persisted: PersistedSettings = {
    apiBaseUrl: settings.apiBaseUrl,
    refreshIntervalMs: settings.refreshIntervalMs,
  };
  localStorage.setItem(SETTINGS_KEY, JSON.stringify(persisted));
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
