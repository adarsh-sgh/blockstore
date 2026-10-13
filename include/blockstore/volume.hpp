#pragma once
#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "blockstore/env.hpp"

namespace blockstore {

constexpr uint32_t kBlockSize = 4096;

struct Options {
  uint64_t blocks = 1 << 18;               // logical size; fixed when the volume is created
  uint64_t segment_bytes = 64ull << 20;    // fixed when the volume is created
  uint32_t capacity_segments = 0;          // physical space; 0 = unbounded (GC never forced)
  uint64_t checkpoint_bytes = 256ull << 20;  // journal size that triggers a checkpoint; 0 = never
  bool compress = true;
  bool dedup = true;
  // Pre-zeroed segment files kept ready (and freed segments recycled into this
  // pool), so group commits overwrite written blocks instead of converting
  // unwritten extents. Off by default: it lowers p50 commit latency but the
  // zero-fill costs bandwidth (see README, "spare segments").
  uint32_t spare_segments = 0;
  uint64_t journal_extend_bytes = 4ull << 20;  // zero-extend the journal ahead of the write pointer
};

struct Corruption : std::runtime_error {
  using std::runtime_error::runtime_error;
};
struct NoSpace : std::runtime_error {
  using std::runtime_error::runtime_error;
};

enum class ReadStatus { Ok, Corrupt };

struct Stats {
  // write path
  uint64_t writes = 0, zero_writes = 0, dedup_hits = 0, new_chunks = 0;
  uint64_t user_bytes_appended = 0;  // stored (compressed) bytes of new chunks
  uint64_t gc_bytes_appended = 0;    // bytes rewritten by GC
  uint64_t pad_bytes = 0;            // alignment padding at group boundaries
  uint64_t groups = 0, journal_bytes = 0, checkpoints = 0;
  uint64_t gc_runs = 0, segments_freed = 0, segments_recycled = 0, segments_zeroed = 0;
  // current state
  uint64_t live_chunks = 0, live_chunk_bytes = 0, segments = 0, mapped_blocks = 0;
  // last open
  uint64_t recovered_groups = 0, recovered_records = 0, dropped_groups = 0;
  double recovery_ms = 0;
};

struct Hash128 {
  uint64_t lo = 0, hi = 0;
  bool operator==(const Hash128& o) const { return lo == o.lo && hi == o.hi; }
};

// Replication hook: the committer hands each group's logical ops to a standby and
// waits for its ack before acknowledging any write in the group.
class GroupSink {
 public:
  virtual ~GroupSink() = default;
  virtual void send(uint64_t seq, const std::vector<uint8_t>& ops) = 0;
  virtual void wait_ack(uint64_t seq) = 0;
};

// A thin-provisioned volume of 4 KiB blocks on a log-structured store.
//
// Data: content-addressed chunks (LZ4 or raw) packed into append-only segment
// files. Metadata: block map pages -> chunk ids -> (segment, offset, len, crc32c).
// Every mutation is a journal record; one committer thread writes each group of
// records plus its segment data and fsyncs both before acking anyone in it.
class Volume {
 public:
  static std::unique_ptr<Volume> open(Env& env, const Options& opt = {});
  ~Volume();
  Volume(const Volume&) = delete;
  Volume& operator=(const Volume&) = delete;

  // Durable on return. Throws IoError / NoSpace; after an IoError the volume is failed.
  void write(uint64_t lba, const void* data);
  // Staged and visible to reads on return; durable once wait(ticket) returns.
  uint64_t write_async(uint64_t lba, const void* data);
  void wait(uint64_t ticket);
  void trim(uint64_t lba);  // the block reads as zeros again

  ReadStatus read(uint64_t lba, void* out, uint32_t snapshot = 0);

  uint32_t snapshot();  // durable on return; id > 0
  void delete_snapshot(uint32_t id);
  std::vector<uint32_t> snapshots();

  void checkpoint();  // write a checkpoint now and start a new journal
  // Re-reads every live chunk and checks its crc32c. Returns the number of corrupt chunks.
  uint64_t scrub();
  // Recomputes every refcount from the maps; throws Corruption on mismatch.
  void check_invariants();

  // Standby side of replication: apply a primary's group, durable on return.
  void apply_replicated(const uint8_t* p, size_t n);
  void set_sink(GroupSink* sink);

  Stats stats();
  uint64_t blocks() const { return opt_.blocks; }
  std::string describe() const { return env_.describe(); }

 private:
  using ChunkId = uint64_t;  // 0 = the zero block
  static constexpr uint32_t kPageEntries = 512;

  struct Chunk {
    uint64_t seg;
    uint32_t off, len, crc;
    bool lz4;
    uint32_t refs;
    Hash128 hash;
  };
  struct MapPage {
    std::array<ChunkId, kPageEntries> e{};
    uint32_t refs = 1;
  };
  using Map = std::vector<MapPage*>;  // nullptr = page of zero blocks
  struct Segment {
    std::shared_ptr<File> file;
    uint64_t live_bytes = 0;
    std::unordered_set<ChunkId> chunks;
  };
  struct Extent {
    uint64_t seg, start;
    AlignedBuf buf;
    std::shared_ptr<File> file;
  };
  struct Group {
    uint64_t seq = 0;
    uint32_t ops = 0;
    std::vector<uint8_t> journal;
    std::vector<Extent> extents;
    std::vector<uint8_t> repl;  // logical ops for the standby
  };
  struct Rec;

  Volume(Env& env, const Options& opt);
  void recover();
  void load_checkpoint(const std::vector<uint8_t>& buf);
  std::vector<uint8_t> encode_checkpoint(uint64_t seq);
  void write_checkpoint(const std::vector<uint8_t>& img, uint64_t seq);
  void open_journal(uint64_t start_seq);

  // State mutations, shared by the live path and journal replay.
  void apply(const Rec& r);
  void log(const Rec& r);
  void map_set(uint64_t lba, ChunkId c);
  void unref_chunk(ChunkId c);
  void unref_page(MapPage* p);
  void seg_drop(uint64_t seg, ChunkId c, uint32_t len);

  struct Prepared;  // a block hashed and compressed outside the lock
  uint64_t stage_write(std::unique_lock<std::mutex>& lk, uint64_t lba, const Prepared& b);
  uint64_t stage_snapshot(uint32_t* id);
  uint64_t stage_delete_snapshot(uint32_t id);
  std::pair<uint64_t, uint32_t> append_payload(std::unique_lock<std::mutex>* lk, const uint8_t* p,
                                               uint32_t n);
  void roll_segment(std::unique_lock<std::mutex>* lk);
  void schedule_delete(uint64_t seg);
  void finish_deletes();
  static ReadStatus load_chunk(const Chunk& c, File& f, uint8_t* out);
  bool copy_from_pending(const Chunk& c, uint8_t* stored) const;
  void notify_work();

  void committer();
  void preparer();
  std::shared_ptr<File> take_spare(uint64_t id);
  void commit(Group& g, GroupSink* sink);
  void gc_step(std::unique_lock<std::mutex>& lk);
  bool gc_wanted() const;
  void check_failed() const;
  static std::string seg_name(uint64_t id);
  static std::string journal_name(uint64_t seq);

  Env& env_;
  Options opt_;

  mutable std::mutex mu_;
  std::condition_variable work_cv_, done_cv_, space_cv_;
  Map live_;
  std::map<uint32_t, Map> snaps_;
  std::unordered_map<ChunkId, Chunk> chunks_;
  struct HashKey {
    size_t operator()(const Hash128& h) const { return static_cast<size_t>(h.lo); }
  };
  std::unordered_map<Hash128, ChunkId, HashKey> index_;
  std::map<uint64_t, Segment> segs_;
  std::vector<std::pair<uint64_t, uint64_t>> to_delete_;  // (segment, durable after seq)
  uint64_t next_chunk_ = 1, next_seg_ = 1;
  uint32_t next_snap_ = 1;
  uint64_t active_seg_ = 0, seg_off_ = 0;  // 0 = none; the first write rolls one

  Group staging_;
  Group* inflight_ = nullptr;
  uint64_t durable_seq_ = 0;
  std::shared_ptr<File> journal_;
  uint64_t journal_off_ = 0, journal_alloc_ = 0;
  std::vector<std::string> spares_;  // pre-zeroed "free-N.dat" files
  uint64_t next_spare_ = 1;
  std::condition_variable spare_cv_;
  std::thread preparer_;
  bool want_checkpoint_ = false, gc_stuck_ = false, replaying_ = false;
  std::atomic<bool> stop_{false};
  std::string failed_;
  GroupSink* sink_ = nullptr;
  Stats st_;
  std::thread committer_;
};

}  // namespace blockstore
