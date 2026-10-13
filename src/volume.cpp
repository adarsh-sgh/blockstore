#include "blockstore/volume.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <unordered_set>

#include "blockstore/crc32c.hpp"
#include "lz4.h"
#define XXH_INLINE_ALL
#include "xxhash.h"

static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "on-disk format is little-endian");

namespace blockstore {

namespace {

constexpr uint32_t kJournalMagic = 0x314a5342;     // "BSJ1"
constexpr uint32_t kCheckpointMagic = 0x31435342;  // "BSC1"
constexpr size_t kFrameHeader = 24;                // magic crc seq len nrec
constexpr uint32_t kGcFreeTarget = 2;              // segments kept free ahead of writers
const char* const kCheckpoint = "checkpoint";

enum ReplOp : uint8_t { kReplWrite = 1, kReplZero = 2, kReplSnap = 3, kReplDelSnap = 4 };

template <class T>
void put(std::vector<uint8_t>& v, T x) {
  size_t n = v.size();
  v.resize(n + sizeof(T));
  std::memcpy(v.data() + n, &x, sizeof(T));
}

struct Reader {
  const uint8_t* p;
  const uint8_t* end;
  bool ok = true;
  template <class T>
  T get() {
    T x{};
    if (static_cast<size_t>(end - p) < sizeof(T)) {
      ok = false;
      return x;
    }
    std::memcpy(&x, p, sizeof(T));
    p += sizeof(T);
    return x;
  }
  const uint8_t* bytes(size_t n) {
    if (static_cast<size_t>(end - p) < n) {
      ok = false;
      return nullptr;
    }
    const uint8_t* r = p;
    p += n;
    return r;
  }
  bool done() const { return p == end; }
};

bool is_zero(const uint8_t* p) {
  uint64_t acc = 0;
  for (size_t i = 0; i < kBlockSize; i += 8) {
    uint64_t v;
    std::memcpy(&v, p + i, 8);
    acc |= v;
  }
  return acc == 0;
}

uint64_t parse_suffix(const std::string& name, const std::string& prefix, const std::string& suffix) {
  if (name.size() <= prefix.size() + suffix.size() || name.compare(0, prefix.size(), prefix) != 0 ||
      name.compare(name.size() - suffix.size(), suffix.size(), suffix) != 0)
    return 0;
  std::string mid = name.substr(prefix.size(), name.size() - prefix.size() - suffix.size());
  if (mid.find_first_not_of("0123456789") != std::string::npos) return 0;
  return std::stoull(mid);
}

std::vector<uint8_t> read_all(File& f) {
  std::vector<uint8_t> v(f.size());
  v.resize(f.pread(v.data(), v.size(), 0));
  return v;
}

}  // namespace

struct Volume::Rec {
  enum Type : uint8_t { NewChunk = 1, Map = 2, Snap = 3, DelSnap = 4, Relocate = 5 };
  Rec() = default;
  Rec(Type t, uint64_t a_, uint64_t b_ = 0) : type(t), a(a_), b(b_) {}
  Type type = Map;
  uint64_t a = 0;  // chunk id (NewChunk, Relocate), lba (Map), snapshot id (Snap, DelSnap)
  uint64_t b = 0;  // chunk id (Map)
  uint64_t seg = 0;
  uint32_t off = 0, len = 0, crc = 0;
  bool lz4 = false;
  Hash128 hash;

  void encode(std::vector<uint8_t>& v) const {
    put<uint8_t>(v, type);
    switch (type) {
      case NewChunk:
        put(v, a), put(v, seg), put(v, off), put(v, len), put(v, crc), put<uint8_t>(v, lz4);
        put(v, hash.lo), put(v, hash.hi);
        break;
      case Map:
        put(v, a), put(v, b);
        break;
      case Snap:
      case DelSnap:
        put(v, static_cast<uint32_t>(a));
        break;
      case Relocate:
        put(v, a), put(v, seg), put(v, off);
        break;
    }
  }

  static bool decode(Reader& r, Rec& x) {
    x = Rec{};
    x.type = static_cast<Type>(r.get<uint8_t>());
    switch (x.type) {
      case NewChunk:
        x.a = r.get<uint64_t>(), x.seg = r.get<uint64_t>(), x.off = r.get<uint32_t>();
        x.len = r.get<uint32_t>(), x.crc = r.get<uint32_t>(), x.lz4 = r.get<uint8_t>() != 0;
        x.hash.lo = r.get<uint64_t>(), x.hash.hi = r.get<uint64_t>();
        break;
      case Map:
        x.a = r.get<uint64_t>(), x.b = r.get<uint64_t>();
        break;
      case Snap:
      case DelSnap:
        x.a = r.get<uint32_t>();
        break;
      case Relocate:
        x.a = r.get<uint64_t>(), x.seg = r.get<uint64_t>(), x.off = r.get<uint32_t>();
        break;
      default:
        return false;
    }
    return r.ok;
  }
};

struct Volume::Prepared {
  const uint8_t* data;
  bool zero;
  Hash128 hash;
  const uint8_t* stored;
  uint32_t len, crc;
  bool lz4;
};

std::string Volume::seg_name(uint64_t id) { return "seg-" + std::to_string(id) + ".dat"; }
std::string Volume::journal_name(uint64_t seq) { return "journal-" + std::to_string(seq) + ".log"; }

Volume::Volume(Env& env, const Options& opt) : env_(env), opt_(opt) {}

std::unique_ptr<Volume> Volume::open(Env& env, const Options& opt) {
  if (opt.segment_bytes % kBlockSize || opt.segment_bytes < 2 * kBlockSize || opt.blocks == 0)
    throw std::invalid_argument("segment_bytes must be a multiple of 4096 (>= 8192), blocks > 0");
  std::unique_ptr<Volume> v(new Volume(env, opt));
  v->recover();
  v->committer_ = std::thread(&Volume::committer, v.get());
  return v;
}

Volume::~Volume() {
  {
    std::lock_guard<std::mutex> g(mu_);
    stop_ = true;
  }
  work_cv_.notify_all();
  if (committer_.joinable()) committer_.join();
  std::unordered_set<MapPage*> pages;
  for (MapPage* p : live_) pages.insert(p);
  for (auto& [id, m] : snaps_)
    for (MapPage* p : m) pages.insert(p);
  for (MapPage* p : pages) delete p;
}

// ---------------------------------------------------------------- state

void Volume::apply(const Rec& r) {
  switch (r.type) {
    case Rec::NewChunk: {
      chunks_[r.a] = Chunk{r.seg, r.off, r.len, r.crc, r.lz4, 0, r.hash};
      if (opt_.dedup) index_[r.hash] = r.a;
      Segment& s = segs_[r.seg];
      s.live_bytes += r.len;
      s.chunks.insert(r.a);
      next_chunk_ = std::max(next_chunk_, r.a + 1);
      next_seg_ = std::max(next_seg_, r.seg + 1);
      break;
    }
    case Rec::Map:
      if (r.a >= opt_.blocks || (r.b && !chunks_.count(r.b))) throw Corruption("map record out of range");
      map_set(r.a, r.b);
      break;
    case Rec::Snap: {
      auto id = static_cast<uint32_t>(r.a);
      Map& m = snaps_[id] = live_;
      for (MapPage* p : m)
        if (p) p->refs++;
      next_snap_ = std::max(next_snap_, id + 1);
      break;
    }
    case Rec::DelSnap: {
      auto it = snaps_.find(static_cast<uint32_t>(r.a));
      if (it == snaps_.end()) throw Corruption("delete of unknown snapshot");
      Map m = std::move(it->second);
      snaps_.erase(it);
      for (MapPage* p : m)
        if (p) unref_page(p);
      break;
    }
    case Rec::Relocate: {
      Chunk& c = chunks_.at(r.a);
      seg_drop(c.seg, r.a, c.len);
      c.seg = r.seg;
      c.off = r.off;
      Segment& s = segs_[r.seg];
      s.live_bytes += c.len;
      s.chunks.insert(r.a);
      next_seg_ = std::max(next_seg_, r.seg + 1);
      break;
    }
  }
}

void Volume::log(const Rec& r) {
  r.encode(staging_.journal);
  apply(r);
}

// Copy-on-write at page granularity: a page shared with a snapshot is copied
// (taking a ref on every chunk it names) before its first modification.
void Volume::map_set(uint64_t lba, ChunkId c) {
  MapPage*& p = live_[lba / kPageEntries];
  if (!p) {
    if (!c) return;
    p = new MapPage();
  } else if (p->refs > 1) {
    auto* q = new MapPage(*p);
    q->refs = 1;
    for (ChunkId e : q->e)
      if (e) chunks_.at(e).refs++;
    p->refs--;
    p = q;
  }
  ChunkId& slot = p->e[lba % kPageEntries];
  ChunkId old = slot;
  if (old == c) return;
  if (c) chunks_.at(c).refs++;
  slot = c;
  if (old) unref_chunk(old);
}

void Volume::unref_page(MapPage* p) {
  if (--p->refs) return;
  for (ChunkId e : p->e)
    if (e) unref_chunk(e);
  delete p;
}

void Volume::unref_chunk(ChunkId id) {
  auto it = chunks_.find(id);
  if (--it->second.refs) return;
  auto ix = index_.find(it->second.hash);
  if (ix != index_.end() && ix->second == id) index_.erase(ix);
  seg_drop(it->second.seg, id, it->second.len);
  chunks_.erase(it);
  gc_stuck_ = false;
}

void Volume::seg_drop(uint64_t seg, ChunkId c, uint32_t len) {
  Segment& s = segs_.at(seg);
  s.live_bytes -= len;
  s.chunks.erase(c);
  if (s.chunks.empty() && seg != active_seg_ && !replaying_) schedule_delete(seg);
}

// A segment file goes only after the records that emptied it are durable.
void Volume::schedule_delete(uint64_t seg) { to_delete_.emplace_back(seg, staging_.seq); }

void Volume::finish_deletes() {
  bool freed = false;
  for (size_t i = 0; i < to_delete_.size();) {
    auto [seg, seq] = to_delete_[i];
    if (seq > durable_seq_) {
      i++;
      continue;
    }
    auto it = segs_.find(seg);
    if (it != segs_.end() && it->second.chunks.empty()) {
      segs_.erase(it);  // open readers keep the unlinked file alive
      env_.remove(seg_name(seg));
      st_.segments_freed++;
      freed = true;
    }
    to_delete_[i] = to_delete_.back();
    to_delete_.pop_back();
  }
  if (freed) space_cv_.notify_all();
}

// ---------------------------------------------------------------- write path

void Volume::check_failed() const {
  if (!failed_.empty()) throw IoError("volume failed: " + failed_);
}

void Volume::notify_work() { work_cv_.notify_one(); }

uint64_t Volume::write_async(uint64_t lba, const void* data) {
  if (lba >= opt_.blocks) throw std::out_of_range("lba out of range");
  thread_local uint8_t comp[LZ4_COMPRESSBOUND(kBlockSize)];
  Prepared b{static_cast<const uint8_t*>(data), false, {}, nullptr, kBlockSize, 0, false};
  b.zero = is_zero(b.data);
  if (!b.zero) {
    XXH128_hash_t h = XXH3_128bits(b.data, kBlockSize);
    b.hash = {h.low64, h.high64};
    b.stored = b.data;
    if (opt_.compress) {
      int n = LZ4_compress_default(reinterpret_cast<const char*>(b.data), reinterpret_cast<char*>(comp),
                                   kBlockSize, sizeof comp);
      if (n > 0 && static_cast<uint32_t>(n) < kBlockSize) {
        b.stored = comp;
        b.len = static_cast<uint32_t>(n);
        b.lz4 = true;
      }
    }
    b.crc = crc32c(b.stored, b.len);
  }
  std::unique_lock<std::mutex> lk(mu_);
  return stage_write(lk, lba, b);
}

uint64_t Volume::stage_write(std::unique_lock<std::mutex>& lk, uint64_t lba, const Prepared& b) {
  check_failed();
  if (b.zero) {
    log({Rec::Map, lba, 0});
    st_.zero_writes++;
  } else {
    ChunkId c = 0;
    if (opt_.dedup) {
      auto it = index_.find(b.hash);
      if (it != index_.end()) c = it->second;
    }
    if (c) {
      st_.dedup_hits++;
    } else {
      auto [seg, off] = append_payload(&lk, b.stored, b.len);  // may wait for GC
      c = next_chunk_;
      Rec r{Rec::NewChunk, c};
      r.seg = seg, r.off = off, r.len = b.len, r.crc = b.crc, r.lz4 = b.lz4, r.hash = b.hash;
      log(r);
      st_.new_chunks++;
      st_.user_bytes_appended += b.len;
    }
    log({Rec::Map, lba, c});
  }
  if (sink_) {
    put<uint8_t>(staging_.repl, b.zero ? kReplZero : kReplWrite);
    put(staging_.repl, lba);
    if (!b.zero) staging_.repl.insert(staging_.repl.end(), b.data, b.data + kBlockSize);
  }
  st_.writes++;
  staging_.ops++;
  notify_work();
  return staging_.seq;
}

std::pair<uint64_t, uint32_t> Volume::append_payload(std::unique_lock<std::mutex>* lk, const uint8_t* p,
                                                     uint32_t n) {
  while (!active_seg_ || seg_off_ + n > opt_.segment_bytes) roll_segment(lk);
  if (staging_.extents.empty() || staging_.extents.back().seg != active_seg_)
    staging_.extents.push_back({active_seg_, seg_off_, AlignedBuf(), segs_.at(active_seg_).file});
  Extent& e = staging_.extents.back();
  size_t pos = seg_off_ - e.start;
  e.buf.resize(pos + n);
  std::memcpy(e.buf.data() + pos, p, n);
  auto at = std::make_pair(active_seg_, static_cast<uint32_t>(seg_off_));
  seg_off_ += n;
  return at;
}

// Writers (lk != null) wait for GC when the volume is at capacity and return
// so the caller re-checks; GC may use one reserve segment so it always progresses.
void Volume::roll_segment(std::unique_lock<std::mutex>* lk) {
  if (opt_.capacity_segments) {
    check_failed();
    if (!lk && segs_.size() > opt_.capacity_segments) throw NoSpace("gc reserve exhausted");
    if (lk && segs_.size() >= opt_.capacity_segments) {
      if (gc_stuck_ && to_delete_.empty()) throw NoSpace("volume full: no reclaimable segment");
      notify_work();
      space_cv_.wait(*lk);
      return;
    }
  }
  uint64_t id = next_seg_;
  auto f = env_.open(seg_name(id), true);
  f->preallocate(opt_.segment_bytes);
  env_.sync_dir();  // the segment must exist after a crash before any record names it
  next_seg_++;
  segs_[id].file = std::move(f);
  uint64_t old = active_seg_;
  active_seg_ = id;
  seg_off_ = 0;
  auto it = segs_.find(old);
  if (it != segs_.end() && it->second.chunks.empty()) schedule_delete(old);
}

void Volume::write(uint64_t lba, const void* data) { wait(write_async(lba, data)); }

void Volume::trim(uint64_t lba) {
  static const uint8_t zero[kBlockSize] = {};
  write(lba, zero);
}

void Volume::wait(uint64_t ticket) {
  std::unique_lock<std::mutex> lk(mu_);
  done_cv_.wait(lk, [&] { return durable_seq_ >= ticket || !failed_.empty(); });
  if (durable_seq_ < ticket) throw IoError("volume failed: " + failed_);
}

uint64_t Volume::stage_snapshot(uint32_t* id) {
  check_failed();
  *id = next_snap_;
  log({Rec::Snap, *id});
  if (sink_) put<uint8_t>(staging_.repl, kReplSnap), put(staging_.repl, *id);
  staging_.ops++;
  notify_work();
  return staging_.seq;
}

uint64_t Volume::stage_delete_snapshot(uint32_t id) {
  check_failed();
  if (!snaps_.count(id)) throw std::out_of_range("no such snapshot");
  log({Rec::DelSnap, id});
  if (sink_) put<uint8_t>(staging_.repl, kReplDelSnap), put(staging_.repl, id);
  staging_.ops++;
  notify_work();
  return staging_.seq;
}

uint32_t Volume::snapshot() {
  uint32_t id;
  uint64_t t;
  {
    std::lock_guard<std::mutex> g(mu_);
    t = stage_snapshot(&id);
  }
  wait(t);
  return id;
}

void Volume::delete_snapshot(uint32_t id) {
  uint64_t t;
  {
    std::lock_guard<std::mutex> g(mu_);
    t = stage_delete_snapshot(id);
  }
  wait(t);
}

std::vector<uint32_t> Volume::snapshots() {
  std::lock_guard<std::mutex> g(mu_);
  std::vector<uint32_t> out;
  for (auto& [id, m] : snaps_) out.push_back(id);
  return out;
}

void Volume::set_sink(GroupSink* sink) {
  std::lock_guard<std::mutex> g(mu_);
  sink_ = sink;
}

void Volume::apply_replicated(const uint8_t* p, size_t n) {
  Reader r{p, p + n};
  uint64_t last = 0;
  static const uint8_t zero[kBlockSize] = {};
  while (!r.done()) {
    auto op = r.get<uint8_t>();
    if (op == kReplWrite || op == kReplZero) {
      auto lba = r.get<uint64_t>();
      const uint8_t* d = op == kReplWrite ? r.bytes(kBlockSize) : zero;
      if (!r.ok) break;
      last = write_async(lba, d);
    } else if (op == kReplSnap || op == kReplDelSnap) {
      auto id = r.get<uint32_t>();
      if (!r.ok) break;
      std::lock_guard<std::mutex> g(mu_);
      if (op == kReplSnap) {
        uint32_t got;
        last = stage_snapshot(&got);
        if (got != id) throw Corruption("replica diverged: snapshot id");
      } else {
        last = stage_delete_snapshot(id);
      }
    } else {
      r.ok = false;
      break;
    }
  }
  if (!r.ok) throw Corruption("malformed replication batch");
  if (last) wait(last);
}

// ---------------------------------------------------------------- read path

bool Volume::copy_from_pending(const Chunk& c, uint8_t* stored) const {
  auto scan = [&](const Group& g) {
    for (auto& e : g.extents)
      if (e.seg == c.seg && c.off >= e.start && c.off + c.len <= e.start + e.buf.size()) {
        std::memcpy(stored, e.buf.data() + (c.off - e.start), c.len);
        return true;
      }
    return false;
  };
  return scan(staging_) || (inflight_ && scan(*inflight_));
}

namespace {
ReadStatus decode_chunk(const uint8_t* stored, uint32_t len, uint32_t crc, bool lz4, uint8_t* out) {
  if (crc32c(stored, len) != crc) return ReadStatus::Corrupt;
  if (!lz4) {
    if (len != kBlockSize) return ReadStatus::Corrupt;
    std::memcpy(out, stored, kBlockSize);
    return ReadStatus::Ok;
  }
  int n = LZ4_decompress_safe(reinterpret_cast<const char*>(stored), reinterpret_cast<char*>(out),
                              static_cast<int>(len), kBlockSize);
  return n == static_cast<int>(kBlockSize) ? ReadStatus::Ok : ReadStatus::Corrupt;
}
}  // namespace

ReadStatus Volume::load_chunk(const Chunk& c, File& f, uint8_t* out) {
  thread_local AlignedBuf buf;
  uint64_t start = c.off, end = c.off + c.len;
  if (f.direct()) start = align_down(start, kBlockSize), end = align_up(end, kBlockSize);
  buf.resize(end - start);
  size_t got = f.pread(buf.data(), end - start, start);
  if (got < c.off + c.len - start) return ReadStatus::Corrupt;
  return decode_chunk(buf.data() + (c.off - start), c.len, c.crc, c.lz4, out);
}

ReadStatus Volume::read(uint64_t lba, void* out, uint32_t snapshot) {
  if (lba >= opt_.blocks) throw std::out_of_range("lba out of range");
  std::unique_lock<std::mutex> lk(mu_);
  const Map* m = &live_;
  if (snapshot) {
    auto it = snaps_.find(snapshot);
    if (it == snaps_.end()) throw std::out_of_range("no such snapshot");
    m = &it->second;
  }
  MapPage* p = (*m)[lba / kPageEntries];
  ChunkId id = p ? p->e[lba % kPageEntries] : 0;
  if (!id) {
    std::memset(out, 0, kBlockSize);
    return ReadStatus::Ok;
  }
  Chunk c = chunks_.at(id);
  uint8_t stored[kBlockSize];
  if (copy_from_pending(c, stored))
    return decode_chunk(stored, c.len, c.crc, c.lz4, static_cast<uint8_t*>(out));
  std::shared_ptr<File> f = segs_.at(c.seg).file;
  lk.unlock();
  return load_chunk(c, *f, static_cast<uint8_t*>(out));
}

uint64_t Volume::scrub() {
  std::vector<ChunkId> ids;
  {
    std::lock_guard<std::mutex> g(mu_);
    for (auto& [id, c] : chunks_) ids.push_back(id);
  }
  uint64_t bad = 0;
  uint8_t out[kBlockSize], stored[kBlockSize];
  for (ChunkId id : ids) {
    std::unique_lock<std::mutex> lk(mu_);
    auto it = chunks_.find(id);
    if (it == chunks_.end() || copy_from_pending(it->second, stored)) continue;
    Chunk c = it->second;
    auto f = segs_.at(c.seg).file;
    lk.unlock();
    bad += load_chunk(c, *f, out) == ReadStatus::Corrupt;
  }
  return bad;
}

// ---------------------------------------------------------------- committer

void Volume::committer() {
  std::unique_lock<std::mutex> lk(mu_);
  for (;;) {
    work_cv_.wait(lk, [&] {
      return stop_ || !staging_.journal.empty() || want_checkpoint_ || gc_wanted() || !failed_.empty();
    });
    if (!failed_.empty()) break;
    if (!stop_ && gc_wanted()) gc_step(lk);
    if (!failed_.empty()) break;
    bool ckpt = want_checkpoint_ || (opt_.checkpoint_bytes && journal_off_ >= opt_.checkpoint_bytes);
    bool has_group = !staging_.journal.empty();
    if (!has_group && !ckpt) {
      if (stop_) break;
      continue;
    }
    Group g;
    if (has_group) {
      g = std::move(staging_);
      staging_ = Group{};
      staging_.seq = g.seq + 1;
      for (auto& e : g.extents) {
        size_t padded = align_up(e.buf.size(), kBlockSize);
        st_.pad_bytes += padded - e.buf.size();
        e.buf.resize(padded);
      }
      if (!g.extents.empty()) seg_off_ = align_up(seg_off_, kBlockSize);
    }
    // A checkpoint taken here covers g, so it is written only after g is durable.
    uint64_t ckpt_seq = has_group ? g.seq : durable_seq_;
    std::vector<uint8_t> img;
    if (ckpt) img = encode_checkpoint(ckpt_seq);
    want_checkpoint_ = false;
    inflight_ = &g;
    GroupSink* sink = sink_;
    lk.unlock();
    std::string err;
    try {
      if (has_group) commit(g, sink);
      if (ckpt) write_checkpoint(img, ckpt_seq);
    } catch (const std::exception& e) {
      err = e.what();
    }
    lk.lock();
    inflight_ = nullptr;
    if (!err.empty()) {
      failed_ = err;
      break;
    }
    if (has_group) {
      durable_seq_ = g.seq;
      st_.groups++;
    }
    if (ckpt) st_.checkpoints++;
    finish_deletes();
    done_cv_.notify_all();
  }
  done_cv_.notify_all();
  space_cv_.notify_all();
}

void Volume::commit(Group& g, GroupSink* sink) {
  std::vector<uint8_t> frame(kFrameHeader);
  frame.insert(frame.end(), g.journal.begin(), g.journal.end());
  uint32_t len = static_cast<uint32_t>(g.journal.size());
  std::memcpy(&frame[0], &kJournalMagic, 4);
  std::memcpy(&frame[8], &g.seq, 8);
  std::memcpy(&frame[16], &len, 4);
  std::memcpy(&frame[20], &g.ops, 4);
  uint32_t crc = crc32c(frame.data() + 8, frame.size() - 8);
  std::memcpy(&frame[4], &crc, 4);

  std::vector<WriteOp> ops;
  for (auto& e : g.extents) ops.push_back({e.file.get(), e.buf.data(), e.buf.size(), e.start});
  ops.push_back({journal_.get(), frame.data(), frame.size(), journal_off_});
  bool repl = sink && !g.repl.empty();
  if (repl) sink->send(g.seq, g.repl);  // the standby works while we do local I/O
  env_.write_and_sync(ops);
  if (repl) sink->wait_ack(g.seq);
  journal_off_ += frame.size();
  std::lock_guard<std::mutex> lk(mu_);
  st_.journal_bytes += frame.size();
}

void Volume::checkpoint() {
  uint64_t target;
  {
    std::unique_lock<std::mutex> lk(mu_);
    check_failed();
    want_checkpoint_ = true;
    target = st_.checkpoints + 1;
    notify_work();
    done_cv_.wait(lk, [&] { return st_.checkpoints >= target || !failed_.empty(); });
    check_failed();
  }
}

// ---------------------------------------------------------------- GC

bool Volume::gc_wanted() const {
  if (!opt_.capacity_segments || gc_stuck_) return false;
  uint64_t used = segs_.size() - to_delete_.size();
  return used + kGcFreeTarget > opt_.capacity_segments;
}

// Greedy: relocate the live chunks of the emptiest sealed segment. Chunk ids do
// not change, so the block maps (and snapshots) are untouched.
void Volume::gc_step(std::unique_lock<std::mutex>& lk) {
  std::unordered_set<uint64_t> busy{active_seg_};
  for (auto& e : staging_.extents) busy.insert(e.seg);
  for (auto& [seg, seq] : to_delete_) busy.insert(seg);
  uint64_t victim = 0, best = UINT64_MAX;
  for (auto& [id, s] : segs_)
    if (!busy.count(id) && s.live_bytes < best) victim = id, best = s.live_bytes;
  if (!victim || best + kBlockSize > opt_.segment_bytes) {
    gc_stuck_ = true;  // cleared when any chunk is freed
    space_cv_.notify_all();
    return;
  }
  struct Live {
    ChunkId id;
    uint32_t off, len;
  };
  std::vector<Live> live;
  uint64_t end = 0;
  for (ChunkId id : segs_[victim].chunks) {
    const Chunk& c = chunks_.at(id);
    live.push_back({id, c.off, c.len});
    end = std::max<uint64_t>(end, c.off + c.len);
  }
  std::sort(live.begin(), live.end(), [](const Live& a, const Live& b) { return a.off < b.off; });
  auto file = segs_[victim].file;
  lk.unlock();
  AlignedBuf buf(align_up(end, kBlockSize));
  std::string err;
  try {
    file->pread(buf.data(), buf.size(), 0);
  } catch (const std::exception& e) {
    err = e.what();
  }
  lk.lock();
  if (!err.empty()) {
    failed_ = err;
    return;
  }
  try {
    for (const Live& l : live) {
      auto it = chunks_.find(l.id);
      if (it == chunks_.end() || it->second.seg != victim || it->second.off != l.off) continue;
      auto [seg, off] = append_payload(nullptr, buf.data() + l.off, l.len);
      Rec r{Rec::Relocate, l.id};
      r.seg = seg, r.off = off;
      log(r);
      st_.gc_bytes_appended += l.len;
    }
  } catch (const NoSpace&) {
    gc_stuck_ = true;
  } catch (const std::exception& e) {
    failed_ = e.what();
  }
  st_.gc_runs++;
}

// ---------------------------------------------------------------- checkpoint + recovery

std::vector<uint8_t> Volume::encode_checkpoint(uint64_t seq) {
  std::vector<uint8_t> v;
  put(v, kCheckpointMagic), put<uint32_t>(v, 0), put<uint64_t>(v, 0);  // magic, crc, len
  put(v, opt_.blocks), put(v, opt_.segment_bytes), put(v, seq);
  put(v, next_chunk_), put(v, next_seg_), put(v, next_snap_);
  put<uint64_t>(v, chunks_.size());
  for (auto& [id, c] : chunks_) {
    put(v, id), put(v, c.seg), put(v, c.off), put(v, c.len), put(v, c.crc), put<uint8_t>(v, c.lz4);
    put(v, c.hash.lo), put(v, c.hash.hi);
  }
  std::unordered_map<MapPage*, uint32_t> idx;
  std::vector<MapPage*> pages;
  auto number = [&](const Map& m) {
    for (MapPage* p : m)
      if (p && idx.emplace(p, static_cast<uint32_t>(pages.size() + 1)).second) pages.push_back(p);
  };
  number(live_);
  for (auto& [id, m] : snaps_) number(m);
  put<uint64_t>(v, pages.size());
  for (MapPage* p : pages) {
    size_t n = v.size();
    v.resize(n + sizeof(p->e));
    std::memcpy(v.data() + n, p->e.data(), sizeof(p->e));
  }
  auto slots = [&](const Map& m) {
    for (MapPage* p : m) put<uint32_t>(v, p ? idx[p] : 0);
  };
  slots(live_);
  put<uint32_t>(v, static_cast<uint32_t>(snaps_.size()));
  for (auto& [id, m] : snaps_) put(v, id), slots(m);
  uint64_t len = v.size() - 16;
  std::memcpy(&v[8], &len, 8);
  uint32_t crc = crc32c(v.data() + 8, v.size() - 8);
  std::memcpy(&v[4], &crc, 4);
  return v;
}

void Volume::load_checkpoint(const std::vector<uint8_t>& buf) {
  Reader r{buf.data(), buf.data() + buf.size()};
  uint32_t magic = r.get<uint32_t>(), crc = r.get<uint32_t>();
  uint64_t len = r.get<uint64_t>();
  if (!r.ok || magic != kCheckpointMagic || len != buf.size() - 16 ||
      crc32c(buf.data() + 8, buf.size() - 8) != crc)
    throw Corruption("checkpoint checksum mismatch");
  opt_.blocks = r.get<uint64_t>();
  opt_.segment_bytes = r.get<uint64_t>();
  durable_seq_ = r.get<uint64_t>();
  next_chunk_ = r.get<uint64_t>(), next_seg_ = r.get<uint64_t>(), next_snap_ = r.get<uint32_t>();
  auto nchunks = r.get<uint64_t>();
  for (uint64_t i = 0; i < nchunks && r.ok; i++) {
    auto id = r.get<uint64_t>();
    Chunk c{};
    c.seg = r.get<uint64_t>(), c.off = r.get<uint32_t>(), c.len = r.get<uint32_t>();
    c.crc = r.get<uint32_t>(), c.lz4 = r.get<uint8_t>() != 0;
    c.hash.lo = r.get<uint64_t>(), c.hash.hi = r.get<uint64_t>();
    chunks_[id] = c;
    if (opt_.dedup) index_[c.hash] = id;
    Segment& s = segs_[c.seg];
    s.live_bytes += c.len;
    s.chunks.insert(id);
  }
  auto npages = r.get<uint64_t>();
  std::vector<MapPage*> pages;
  for (uint64_t i = 0; i < npages && r.ok; i++) {
    const uint8_t* src = r.bytes(sizeof(MapPage::e));
    if (!src) break;
    auto* p = new MapPage();
    p->refs = 0;
    std::memcpy(p->e.data(), src, sizeof(p->e));
    pages.push_back(p);
    for (ChunkId e : p->e) {
      auto it = chunks_.find(e);
      if (e && it == chunks_.end()) r.ok = false;
      if (e && it != chunks_.end()) it->second.refs++;
    }
  }
  size_t nslots = (opt_.blocks + kPageEntries - 1) / kPageEntries;
  auto read_map = [&](Map& m) {
    m.assign(nslots, nullptr);
    for (auto& slot : m) {
      auto i = r.get<uint32_t>();
      if (i > pages.size()) r.ok = false;
      if (!r.ok) return;
      if (i) (slot = pages[i - 1])->refs++;
    }
  };
  read_map(live_);
  auto nsnaps = r.get<uint32_t>();
  for (uint32_t i = 0; i < nsnaps && r.ok; i++) read_map(snaps_[r.get<uint32_t>()]);
  for (MapPage* p : pages)
    if (!p->refs) delete p;  // unreachable only if the image is corrupt
  if (!r.ok || !r.done()) throw Corruption("checkpoint malformed");
}

void Volume::write_checkpoint(const std::vector<uint8_t>& img, uint64_t seq) {
  env_.remove("checkpoint.tmp");
  {
    auto f = env_.open("checkpoint.tmp");
    f->pwrite(img.data(), img.size(), 0);
    f->sync();
  }
  env_.rename("checkpoint.tmp", kCheckpoint);
  env_.sync_dir();
  std::string keep = journal_name(seq + 1);
  open_journal(seq + 1);
  for (auto& n : env_.list())
    if (n != keep && parse_suffix(n, "journal-", ".log")) env_.remove(n);
  env_.sync_dir();
}

void Volume::open_journal(uint64_t start_seq) {
  journal_ = env_.open(journal_name(start_seq));
  journal_off_ = 0;
}

void Volume::recover() {
  auto t0 = std::chrono::steady_clock::now();
  if (!env_.exists(kCheckpoint)) {  // fresh volume
    live_.assign((opt_.blocks + kPageEntries - 1) / kPageEntries, nullptr);
    write_checkpoint(encode_checkpoint(0), 0);
    staging_.seq = 1;
    return;
  }
  replaying_ = true;
  load_checkpoint(read_all(*env_.open(kCheckpoint)));
  const uint64_t ckpt_seq = durable_seq_;

  std::vector<std::pair<uint64_t, std::string>> journals;
  for (auto& n : env_.list())
    if (uint64_t s = parse_suffix(n, "journal-", ".log")) journals.emplace_back(s, n);
  std::sort(journals.begin(), journals.end());

  struct Frame {
    uint64_t seq;
    size_t file, off, end;
    std::vector<uint8_t> payload;
  };
  std::vector<Frame> frames;
  std::vector<std::vector<uint8_t>> images;
  auto parse = [&](const std::vector<uint8_t>& img, size_t off, Frame& f) {
    if (img.size() - off < kFrameHeader) return false;
    uint32_t magic, crc, len;
    std::memcpy(&magic, &img[off], 4);
    std::memcpy(&crc, &img[off + 4], 4);
    std::memcpy(&f.seq, &img[off + 8], 8);
    std::memcpy(&len, &img[off + 16], 4);
    if (magic != kJournalMagic || img.size() - off - kFrameHeader < len) return false;
    if (crc32c(&img[off + 8], kFrameHeader - 8 + len) != crc) return false;
    f.off = off;
    f.end = off + kFrameHeader + len;
    f.payload.assign(img.begin() + static_cast<long>(off + kFrameHeader), img.begin() + static_cast<long>(f.end));
    return true;
  };
  uint64_t expect = ckpt_seq + 1;
  for (size_t j = 0; j < journals.size(); j++) {
    images.push_back(read_all(*env_.open(journals[j].second)));
    const auto& img = images.back();
    size_t off = 0;
    Frame f;
    while (parse(img, off, f)) {
      if (f.seq > expect) throw Corruption("journal gap before seq " + std::to_string(f.seq));
      off = f.end;
      if (f.seq == expect) {
        f.file = j;
        frames.push_back(std::move(f));
        expect++;
      }
      f = Frame{};
    }
    // A bad frame is a torn tail only if nothing valid follows it.
    for (size_t k = off + 1; k + kFrameHeader <= img.size(); k++) {
      Frame g;
      if (img[k] == (kJournalMagic & 0xff) && parse(img, k, g) && g.seq >= expect)
        throw Corruption("journal corrupt at " + journals[j].second + "+" + std::to_string(off));
    }
  }

  auto decode_all = [](const std::vector<uint8_t>& p) {
    std::vector<Rec> recs;
    Reader r{p.data(), p.data() + p.size()};
    while (!r.done()) {
      Rec x;
      if (!Rec::decode(r, x)) throw Corruption("bad journal record");
      recs.push_back(x);
    }
    return recs;
  };

  // Groups commit one at a time, so only the last one can have a journal frame
  // whose segment data never became durable. Verify its payloads; drop it if torn.
  if (!frames.empty()) {
    auto last = decode_all(frames.back().payload);
    std::unordered_map<ChunkId, std::pair<uint32_t, uint32_t>> len_crc;  // id -> (len, crc)
    bool relocates = false;
    for (auto& x : last) relocates |= x.type == Rec::Relocate;
    if (relocates) {
      for (auto& [id, c] : chunks_) len_crc[id] = {c.len, c.crc};
      for (size_t i = 0; i + 1 < frames.size(); i++)
        for (auto& x : decode_all(frames[i].payload))
          if (x.type == Rec::NewChunk) len_crc[x.a] = {x.len, x.crc};
    }
    bool ok = true;
    for (auto& x : last) {
      if (x.type == Rec::NewChunk) len_crc[x.a] = {x.len, x.crc};
      if (x.type != Rec::NewChunk && x.type != Rec::Relocate) continue;
      if (!env_.exists(seg_name(x.seg))) continue;  // removed, so its group was durable
      auto lc = len_crc.find(x.a);
      if (lc == len_crc.end()) throw Corruption("relocation of unknown chunk");
      std::vector<uint8_t> b(lc->second.first);
      ok &= env_.open(seg_name(x.seg))->pread(b.data(), b.size(), x.off) == b.size() &&
            crc32c(b.data(), b.size()) == lc->second.second;
    }
    if (!ok) {
      // Wipe the frame header durably so the frame cannot resurface after its
      // segment has been cleaned up as an orphan.
      auto jf = env_.open(journals[frames.back().file].second);
      std::vector<uint8_t> z(kFrameHeader, 0);
      jf->pwrite(z.data(), z.size(), frames.back().off);
      jf->sync();
      frames.pop_back();
      st_.dropped_groups++;
    }
  }

  for (auto& f : frames) {
    auto recs = decode_all(f.payload);
    for (auto& x : recs) apply(x);
    st_.recovered_records += recs.size();
    durable_seq_ = f.seq;
  }
  st_.recovered_groups = frames.size();
  replaying_ = false;

  // Continue the journal right after the last good frame (a dropped or torn
  // frame gets overwritten); with no frame past the checkpoint, start the next file.
  if (!frames.empty()) {
    journal_ = env_.open(journals[frames.back().file].second);
    journal_off_ = frames.back().end;
  } else {
    open_journal(ckpt_seq + 1);
    env_.sync_dir();
  }
  staging_.seq = durable_seq_ + 1;

  for (auto it = segs_.begin(); it != segs_.end();)
    it = it->second.chunks.empty() ? segs_.erase(it) : std::next(it);
  for (auto& n : env_.list())
    if (uint64_t id = parse_suffix(n, "seg-", ".dat")) {
      if (!segs_.count(id)) env_.remove(n);
      next_seg_ = std::max(next_seg_, id + 1);
    }
  env_.sync_dir();
  for (auto& [id, s] : segs_) s.file = env_.open(seg_name(id), true);
  st_.recovery_ms =
      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
}

void Volume::check_invariants() {
  std::lock_guard<std::mutex> g(mu_);
  std::unordered_map<MapPage*, uint32_t> page_refs;
  auto count = [&](const Map& m) {
    if (m.size() != (opt_.blocks + kPageEntries - 1) / kPageEntries) throw Corruption("map size");
    for (MapPage* p : m)
      if (p) page_refs[p]++;
  };
  count(live_);
  for (auto& [id, m] : snaps_) count(m);
  std::unordered_map<ChunkId, uint32_t> chunk_refs;
  for (auto& [p, n] : page_refs) {
    if (p->refs != n) throw Corruption("page refcount mismatch");
    for (ChunkId e : p->e)
      if (e) chunk_refs[e]++;
  }
  if (chunk_refs.size() != chunks_.size()) throw Corruption("unreferenced or missing chunks");
  std::map<uint64_t, uint64_t> seg_bytes;
  for (auto& [id, c] : chunks_) {
    auto it = chunk_refs.find(id);
    if (it == chunk_refs.end() || it->second != c.refs) throw Corruption("chunk refcount mismatch");
    if (!segs_.count(c.seg) || !segs_.at(c.seg).chunks.count(id)) throw Corruption("chunk not in segment");
    seg_bytes[c.seg] += c.len;
  }
  for (auto& [id, s] : segs_)
    if (s.live_bytes != seg_bytes[id] || s.chunks.size() > chunks_.size())
      throw Corruption("segment accounting mismatch");
}

Stats Volume::stats() {
  std::lock_guard<std::mutex> g(mu_);
  Stats s = st_;
  s.live_chunks = chunks_.size();
  s.segments = segs_.size();
  for (auto& [id, seg] : segs_) s.live_chunk_bytes += seg.live_bytes;
  for (MapPage* p : live_)
    if (p)
      for (ChunkId e : p->e) s.mapped_blocks += e != 0;
  return s;
}

}  // namespace blockstore
