export interface PagedResult<T> {
  items: T[];
  totalCount: number;
  page: number;
  pageSize: number;
}

export interface OverviewInfo {
  clusterStatus: string;
  connectionCount: number;
  healthyConnections: number;
  topicCount: number;
  queueCount: number;
  totalQueueDepth: number;
  activeConsumers: number;
  pendingRetries: number;
  deadLetterCount: number;
  messagesPublishedPerSecond: number;
  messagesConsumedPerSecond: number;
  generatedAt: string;
}

export interface ConnectionInfo {
  id: string;
  name: string;
  type: string;
  endpoint?: string;
  status: string;
  latencyMs?: number;
  lastCheckedAt: string;
  error?: string;
}

export interface TopicInfo {
  name: string;
  transport: string;
  type: string;
  partitions: number;
  messageCount: number;
  publishRate: number;
  createdAt: string;
}

export interface QueueInfo {
  name: string;
  transport: string;
  depth: number;
  inFlight: number;
  consumerCount: number;
  durable: boolean;
  deadLetterDestination?: string;
}

export interface MessageInfo {
  id: string;
  type: string;
  source: string;
  transport: string;
  correlationId?: string;
  timestamp: string;
  sizeBytes: number;
  status: string;
  payloadPreview?: string;
  headers: Record<string, string>;
}

export interface ConsumerInfo {
  id: string;
  name: string;
  destination: string;
  transport: string;
  status: string;
  concurrency: number;
  messagesProcessed: number;
  startedAt: string;
  lastMessageAt?: string;
}

export interface RetryInfo {
  id: string;
  messageId: string;
  destination: string;
  transport: string;
  attempt: number;
  maxAttempts: number;
  nextRetryAt?: string;
  failureReason?: string;
  createdAt: string;
}

export interface DeadLetterInfo {
  id: string;
  messageId: string;
  type: string;
  originalDestination: string;
  deadLetterDestination: string;
  transport: string;
  failureReason?: string;
  deliveryAttempts: number;
  deadLetteredAt: string;
}

export interface ReplayJobInfo {
  id: string;
  source: string;
  destination?: string;
  status: string;
  messagesReplayed: number;
  totalMessages?: number;
  createdAt: string;
  completedAt?: string;
  error?: string;
}

export interface ReplayRequest {
  source: string;
  destination?: string;
  from?: string;
  to?: string;
  maxMessages?: number;
  filter?: string;
}

export interface MetricValue {
  name: string;
  type: string;
  value: number;
  tags: Record<string, string>;
  unit?: string;
  description?: string;
}

export interface MetricsSnapshot {
  capturedAt: string;
  metrics: MetricValue[];
}

export interface PluginInfo {
  name: string;
  version: string;
  description?: string;
  author?: string;
  enabled: boolean;
  tags: string[];
  status: string;
}

export interface ComponentHealth {
  name: string;
  status: string;
  description?: string;
  metadata: Record<string, string>;
}

export interface ClusterHealthInfo {
  status: string;
  version: string;
  checkedAt: string;
  components: ComponentHealth[];
}

export interface DashboardSettings {
  apiBaseUrl: string;
  apiKey: string;
  refreshIntervalMs: number;
}
