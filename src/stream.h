#ifndef KUTTIDB_STREAM_H
#define KUTTIDB_STREAM_H

#include <stddef.h>
#include <stdint.h>

#define STREAM_NAME_MAX 255u
#define STREAM_GROUP_MAX 255u
#define STREAM_PARTITIONS_MAX 256u
#define STREAM_FETCH_MAX 1024u
#define STREAM_GROUP_MEMBERS_MAX 1024u

typedef struct StreamStore StreamStore;

typedef struct StreamRecordView {
    uint64_t offset;
    const void *key;
    uint16_t key_len;
    const void *data;
    uint32_t len;
} StreamRecordView;

typedef struct StreamAppendInput {
    const void *key;
    uint16_t key_len;
    const void *data;
    uint32_t len;
} StreamAppendInput;

typedef struct StreamCommitInput {
    uint32_t partition;
    uint64_t offset;
} StreamCommitInput;

typedef enum StreamOffsetResetStrategy {
    STREAM_OFFSET_RESET_EARLIEST,
    STREAM_OFFSET_RESET_LATEST,
    STREAM_OFFSET_RESET_ABSOLUTE,
    STREAM_OFFSET_RESET_RELATIVE
} StreamOffsetResetStrategy;

typedef struct StreamAppendResult {
    uint64_t partition;
    uint64_t offset;
} StreamAppendResult;

typedef struct StreamPartitionStats {
    uint64_t base_offset;
    uint64_t next_offset;
    uint64_t retained_bytes;
} StreamPartitionStats;

/* A durable topic declaration is idempotent. `max_bytes` and `max_age_ms`
 * are retention ceilings (zero means unlimited). Appends are acknowledged
 * only after their CRC-checked WAL record has been fsynced. */
StreamStore *stream_store_open(const char *path);
void stream_store_close(StreamStore *store);
int stream_declare(StreamStore *store, const char *name, uint32_t name_len,
                   uint32_t partitions, uint64_t max_bytes, uint64_t max_age_ms);
int stream_append(StreamStore *store, const char *name, uint32_t name_len,
                  uint32_t partition_hint, const void *key, uint16_t key_len,
                  const void *data, uint32_t len, uint64_t *out_partition,
                  uint64_t *out_offset);
/* Appends one bounded batch under one fsynced WAL record. No crash can leave
 * a partially persisted batch (normal retention may immediately evict records).
 * Every input uses the same explicit partition or keyed selection when hint
 * is UINT32_MAX. */
int stream_append_batch(StreamStore *store, const char *name, uint32_t name_len,
                        uint32_t partition_hint, const StreamAppendInput *inputs,
                        uint32_t count, StreamAppendResult *out_results);
/* Returns 1 when the topic/partition exists, 0 when it does not, -2 when the
 * first eligible record cannot fit within `max_bytes`, and -1 on an invalid
 * request or allocation failure. Returned views remain valid until
 * stream_fetch_free and are copied out of the store while the lock is held. */
int stream_fetch(StreamStore *store, const char *name, uint32_t name_len,
                 uint32_t partition, uint64_t offset, uint32_t max_records,
                 uint64_t max_bytes, StreamRecordView **out_records,
                 uint32_t *out_count);
void stream_fetch_free(StreamRecordView *records, uint32_t count);
/* Durably remove records below `base_offset` from one partition using the
 * native trim WAL record. The expected topic revision is checked under the
 * Stream lock. Returns 0 on success, 1 stale revision, 2 invalid retained
 * boundary, 3 unknown topic/partition, or -1 on persistence failure. */
int stream_truncate_if_revision(StreamStore *store, const char *name,
                                uint32_t name_len, uint32_t partition,
                                uint64_t base_offset,
                                uint64_t expected_revision);
/* Conditionally replace a Stream's retention ceilings.  The new values are
 * durably recorded before becoming visible; any records newly outside the
 * ceiling are trimmed as part of the same durability group.  Returns 0 on
 * success, 1 stale revision, 3 unknown Stream, or -1 on invalid input or a
 * persistence failure. */
int stream_set_retention_if_revision(StreamStore *store, const char *name,
                                     uint32_t name_len, uint64_t max_bytes,
                                     uint64_t max_age_ms,
                                     uint64_t expected_revision);
/* Conditionally and durably delete a Stream, including all retained records
 * and consumer-group offsets. Returns 0 on success, 1 stale revision, 3
 * unknown Stream, or -1 on invalid input or a persistence failure. */
int stream_delete_if_revision(StreamStore *store, const char *name,
                              uint32_t name_len, uint64_t expected_revision);
int stream_revision(StreamStore *store, const char *name, uint32_t name_len,
                    uint64_t *out_revision);
/* Copy immutable declaration fields and the current revision under one
 * Stream lock. Returns 1 when the Stream exists, 0 when it does not, or -1
 * for invalid arguments. */
int stream_config_snapshot(StreamStore *store, const char *name,
                           uint32_t name_len, uint32_t *out_partitions,
                           uint64_t *out_max_bytes, uint64_t *out_max_age_ms,
                           uint64_t *out_revision);
/* Copies one topic's bounded partition-boundary snapshot while holding the
 * Stream lock once. `out_cap` must cover the topic partition count; no record
 * bodies or private Stream pointers escape. Returns 1 when the topic exists,
 * 0 when it does not, or -1 for invalid arguments/capacity. */
int stream_partition_snapshot(StreamStore *store, const char *name,
                              uint32_t name_len, StreamPartitionStats *out,
                              uint32_t out_cap, uint32_t *out_count,
                              uint64_t *out_revision);
/* Persist a consumer group's next offset. Group membership/assignment stays
 * deliberately outside this initial native API; clients may replay arbitrary
 * offsets and commit independently. */
int stream_commit(StreamStore *store, const char *name, uint32_t name_len,
                  const char *group, uint32_t group_len, uint32_t partition,
                  uint64_t offset);
int stream_group_offset(StreamStore *store, const char *name, uint32_t name_len,
                        const char *group, uint32_t group_len, uint32_t partition,
                        uint64_t *out_offset);
int stream_group_lag(StreamStore *store, const char *name, uint32_t name_len,
                     const char *group, uint32_t group_len, uint32_t partition,
                     uint64_t *out_lag);
/* Read the current membership generation without joining or changing a
 * group.  Returns 1 when the group exists, 0 when it does not, or -1 for
 * invalid input. */
int stream_group_generation(StreamStore *store, const char *name,
                            uint32_t name_len, const char *group,
                            uint32_t group_len, uint64_t *out_generation);
/* Administrative conditional offset commit.  The group must already exist;
 * it is never created by this API.  The offset is checked atomically against
 * the partition's retained [base, next] range and the supplied membership
 * generation.  Returns 0 on durable success, 1 for a stale generation, 2
 * for an out-of-range offset, 3 for an unknown topic/group/partition, and
 * -1 for invalid input, allocation, or persistence failure. */
int stream_commit_if_generation(StreamStore *store, const char *name,
                                uint32_t name_len, const char *group,
                                uint32_t group_len, uint32_t partition,
                                uint64_t offset, uint64_t expected_generation);
/* Conditional bounded administrative batch commit. All entries are validated
 * (including retained ranges) under one Stream lock before any WAL record is
 * written; a successful result has one shared fsync. Return codes mirror
 * stream_commit_if_generation: 1 stale generation, 2 invalid range, 3
 * unknown stream/group/partition, and -1 persistence/allocation failure. */
int stream_commit_batch_if_generation(StreamStore *store, const char *name,
                                      uint32_t name_len, const char *group,
                                      uint32_t group_len,
                                      const StreamCommitInput *inputs,
                                      uint32_t count,
                                      uint64_t expected_generation);
/* Reset every partition in an existing Consumer Group under one Stream lock.
 * `absolute` is used by ABSOLUTE and `delta` by RELATIVE.  The operation
 * validates generation, active membership, arithmetic, and every retained
 * boundary before its first WAL write, then persists all offsets with one
 * durability wait.  `old_offsets` and `new_offsets` may be NULL; otherwise
 * each must have room for all partitions.  Returns 0 on success, 1 stale
 * generation, 2 a target outside retained range, 3 unknown stream/group,
 * 4 active group without force, and -1 on invalid input or persistence error. */
int stream_group_reset_offsets_if_generation(
    StreamStore *store, const char *name, uint32_t name_len,
    const char *group, uint32_t group_len, uint64_t expected_generation,
    StreamOffsetResetStrategy strategy, uint64_t absolute, int64_t delta,
    int force, uint64_t *old_offsets, uint64_t *new_offsets,
    uint32_t out_cap, uint32_t *out_count);
/* Group membership is ephemeral and lease-based. Joining refreshes the
 * caller's lease and returns its deterministic partition assignment. Members
 * are rebalanced whenever a lease expires or a connection leaves. The
 * returned generation increases on every membership change (join, graceful
 * leave, disconnect, lease expiry) and is unchanged by a plain heartbeat, so
 * callers can detect rebalances without diffing assignments. */
int stream_group_join(StreamStore *store, const char *name, uint32_t name_len,
                      const char *group, uint32_t group_len, uint64_t owner,
                      uint32_t lease_ms, uint32_t **out_partitions,
                      uint32_t *out_count, uint64_t *out_generation);
/* Graceful leave: one member releases one group's partitions immediately so
 * the others rebalance without waiting for the lease to expire. A member
 * that drained its in-flight work must leave this way. */
int stream_group_leave_member(StreamStore *store, const char *name,
                              uint32_t name_len, const char *group,
                              uint32_t group_len, uint64_t owner);
void stream_group_leave_owner(StreamStore *store, uint64_t owner);
void stream_reap(StreamStore *store);
/* Return 1 when this live member currently owns the partition, 0 when the
 * topic/group/member/assignment is absent, or -1 on invalid arguments. */
int stream_group_member_assigned(StreamStore *store, const char *name,
                                 uint32_t name_len, const char *group,
                                 uint32_t group_len, uint64_t owner,
                                 uint32_t partition, uint64_t *out_generation);
/* Server-side commits must come from a live member assigned the partition. */
int stream_commit_for_owner(StreamStore *store, const char *name, uint32_t name_len,
                            const char *group, uint32_t group_len,
                            uint32_t partition, uint64_t offset, uint64_t owner);
/* Conditional owner-aware commit for Management sessions. It checks the
 * current generation, assignment, and retained range atomically. Returns 0
 * on success, 1 stale generation, 2 out-of-range offset, 3 absent or
 * unassigned member/partition, and -1 on persistence failure. */
int stream_commit_for_owner_if_generation(StreamStore *store, const char *name,
                                          uint32_t name_len, const char *group,
                                          uint32_t group_len, uint32_t partition,
                                          uint64_t offset, uint64_t owner,
                                          uint64_t expected_generation);
/* Batch offset commit (protocol 0x6b): all commits share one group fsync
 * before the response. With owner == 0 the group is created on demand and no
 * assignment is validated (plain stream_commit semantics); otherwise every
 * partition must be assigned to the owner, and the group must already exist.
 * Inputs are applied in order under one lock hold; later entries for the
 * same partition win. Returns 0, or -1 on an invalid request, unknown
 * topic/group/partition, an assignment failure, or persistence failure. */
int stream_commit_batch_for_owner(StreamStore *store, const char *name,
                                  uint32_t name_len, const char *group,
                                  uint32_t group_len, uint64_t owner,
                                  const StreamCommitInput *inputs,
                                  uint32_t count);
uint64_t stream_topic_count(StreamStore *store);
/* Per-topic labeled metrics snapshot, taken under the store lock: retained
 * record bytes and the number of retained records across all partitions. */
typedef void (*StreamTopicStatsFn)(const char *name, uint32_t name_len,
                                   uint32_t partitions, uint64_t bytes,
                                   uint64_t records, void *ud);
void stream_foreach_stats(StreamStore *store, StreamTopicStatsFn fn, void *ud);
/* Consumer-group inventory snapshot, taken under the store lock; expired
 * members are reaped before their group is reported. */
typedef void (*StreamGroupStatsFn)(const char *topic, uint32_t topic_len,
                                   const char *group, uint32_t group_len,
                                   uint64_t generation, uint32_t members,
                                   void *ud);
void stream_group_foreach_stats(StreamStore *store, StreamGroupStatsFn fn,
                                void *ud);
/* Snapshot a group's live membership without exporting native owner tokens.
 * Members are reported in deterministic assignment order after expired leases
 * are reaped. `member_index` is opaque and stable only within this snapshot;
 * `assigned_partitions` is derived from the current rebalance. Returns 1
 * when the group exists, 0 when it does not, and -1 for invalid arguments. */
typedef void (*StreamGroupMemberSnapshotFn)(uint32_t member_index,
                                            uint32_t assigned_partitions,
                                            uint64_t lease_remaining_ms,
                                            void *ud);
int stream_group_member_snapshot(StreamStore *store, const char *name,
                                 uint32_t name_len, const char *group,
                                 uint32_t group_len,
                                 StreamGroupMemberSnapshotFn fn, void *ud,
                                 uint64_t *out_generation,
                                 uint32_t *out_member_count);
uint64_t stream_partition_count(StreamStore *store);
uint64_t stream_retention_bytes(StreamStore *store);
uint64_t stream_group_count(StreamStore *store);
uint64_t stream_group_member_count(StreamStore *store);
int stream_persistence_failed(StreamStore *store);
/* Run a crash-safe Stream WAL compaction when history exceeds the live-state
 * threshold. Returns 1 when compacted, 0 when no checkpoint is needed, and
 * -1 when persistence is unavailable or the checkpoint fails. */
int stream_checkpoint_maybe(StreamStore *store);
/* Diagnostic full recount, O(retained records): walks every retained record
 * and recomputes the live checkpoint-size estimate from scratch. Tests
 * compare `full_live` against `incr_live`, the incrementally maintained
 * estimate that drives WAL compaction eligibility, and `full_records`
 * against the per-topic counters reported by stream_foreach_stats. */
void stream_debug_recount(StreamStore *store, uint64_t *full_records,
                          uint64_t *full_live, uint64_t *incr_live);

#endif
