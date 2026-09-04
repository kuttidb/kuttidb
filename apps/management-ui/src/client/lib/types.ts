/** Response shapes observed from the live Management API v1. Additive fields are tolerated. */

export type StatusShape = {
  uptime_seconds: number;
  ready: boolean;
  server_version: string;
  event_loops: number;
  event_backend: string;
  durability: string;
  keyspace: { entry_count: number; live_bytes: number; allocated_bytes: number; expired_count: number; evicted_count: number };
  queues: { count: number; ready_depth: number; in_flight: number; redelivery_count: number; dead_letter_count: number; persistence_healthy: boolean };
  streams: { count: number; partition_count: number; retained_bytes: number; group_count: number; member_count: number; persistence_healthy: boolean };
  management: { active_tails: number; active_deliveries: number; active_claims: number; queued_jobs: number; running_jobs: number; mutation_attempts: number; audit_failures: number; rate_limit_rejections: number; operation_in_doubt: number };
  audit: { healthy: boolean };
  persistence_healthy: boolean;
};

export type MaintenanceEntry = { engine: string; checkpoint_available: boolean };

export type JobEntry = {
  job_id: string;
  kind: string;
  state: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  created_at: number;
  completed_at?: number | null;
  error?: { code?: string; message?: string } | null;
  result?: Record<string, unknown> | null;
};

export type BinaryField = { encoding: string; data: string; size?: number; content_type?: string };

export type KeyspaceInfo = {
  id: string; name: string; entry_count: number; live_bytes: number; allocated_bytes: number;
  expired_count: number; evicted_count: number; revision: number; persistence_healthy: boolean;
};

export type KeyspaceEntrySummary = {
  id: string; key: string; key_encoding: string; value_size: number;
  expires_at: number | null; remaining_ttl_ms: number | null;
};

export type KeyspaceEntry = {
  id: string; key: string; key_encoding: string;
  value: BinaryField; revision: number;
};

export type QueueSummary = { id: string; name: string; name_encoding: string; ready_depth: number; in_flight: number };

export type QueueDetail = {
  id: string; name: string; name_encoding: string; durable: boolean;
  max_depth: number; max_deliveries: number; dead_letter_queue: string | null;
  ready_depth: number; in_flight: number; revision: number;
};

export type QueueMessage = {
  message_id: number; state: "ready" | "delayed" | "in-flight"; size: number;
  expires_at_ms: number | null; delivery_count: number; redelivered: boolean; body?: BinaryField;
};

export type DeliveryReceipt = { delivery_id: string; message_id: number; body?: BinaryField };

export type DeliveryDetail = {
  delivery_id: string; message_id: number;
  queue: { id: string; name: string; name_encoding: string };
  state: string; lease_remaining_ms: number; body?: BinaryField;
};

export type QueueConsumer = { id: string; name: string; name_encoding: string };

export type StreamSummary = {
  id: string; name: string; name_encoding: string; partition_count: number;
  retained_bytes: number; retained_record_count: number;
};

export type StreamDetail = {
  id: string; name: string; name_encoding: string; partition_count: number;
  max_retained_bytes: number; max_retained_age_ms: number;
  retained_bytes: number; retained_record_count: number; revision: number;
};

export type StreamPartition = { partition: number; base_offset: number; next_offset: number; retained_bytes: number };

export type StreamRecord = { partition: number; offset: number; key?: BinaryField; body: BinaryField };

export type GroupOffsetsEntry = { partition: number; offset: number; high_water_offset?: number; lag: number };

export type ConsumerGroupDetail = {
  id: string; stream: string; group: string; generation: number; active_member_count: number;
  offsets: GroupOffsetsEntry[];
};

export type ConsumerGroupListItem = {
  stream: string; group: string; group_encoding?: string; generation: number; active_member_count: number;
};

export type GroupMember = { member_id: string; assigned_partition_count: number; lease_remaining_ms: number };

export type RouterSummary = {
  id: string; name: string; name_encoding: string; mode: "exact" | "broadcast" | "pattern";
  durable: boolean; route_count: number; revision: number;
  publish_attempt_count: number; unroutable_count: number; metrics_scope: string;
  alternate_router?: string | null;
};

export type RouteEntry = {
  route_id: string;
  queue: { id: string; name: string; name_encoding: string };
  routing_key: { id: string; value: string; value_encoding: string };
};
