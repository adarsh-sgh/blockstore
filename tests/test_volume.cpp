#include <map>
#include <thread>

#include "blockstore/crc32c.hpp"
#include "doctest.h"
#include "util.hpp"

using namespace blockstore;
using namespace testutil;

namespace {

struct Model {
  std::map<uint64_t, Block> live;
  std::map<uint32_t, std::map<uint64_t, Block>> snaps;
};

void verify(Volume& v, const Model& m, uint64_t blocks) {
  Block got(kBlockSize), zero(kBlockSize, 0);
  for (uint64_t lba = 0; lba < blocks; lba++) {
    REQUIRE(v.read(lba, got.data()) == ReadStatus::Ok);
    auto it = m.live.find(lba);
    REQUIRE_MESSAGE(got == (it == m.live.end() ? zero : it->second), "live lba " << lba);
  }
  for (auto& s : m.snaps)
    for (uint64_t lba = 0; lba < blocks; lba++) {
      REQUIRE(v.read(lba, got.data(), s.first) == ReadStatus::Ok);
      auto it = s.second.find(lba);
      REQUIRE_MESSAGE(got == (it == s.second.end() ? zero : it->second), "snap " << s.first << " lba " << lba);
    }
  v.check_invariants();
}

}  // namespace

TEST_CASE("volume: random writes, snapshots, GC and reopen agree with a model") {
  SimEnv env;
  Options o;
  o.blocks = 2048;  // 4 map pages
  o.segment_bytes = 64 << 10;
  o.capacity_segments = 160;  // 10 MiB physical for 8 MiB logical: GC runs constantly
  o.checkpoint_bytes = 256 << 10;
  std::mt19937_64 rng(42);
  Model m;
  auto v = Volume::open(env, o);
  uint64_t tag = 0;

  for (int round = 0; round < 30; round++) {
    // A burst of async writes, so groups hold many records.
    std::vector<uint64_t> tickets;
    for (int i = 0; i < 400; i++) {
      uint64_t lba = rng() % (round % 3 == 0 ? o.blocks : 300);  // hot range forces overwrites
      int kind = pick_kind(rng);
      Block b = make_block(rng(), kind, ++tag);
      tickets.push_back(v->write_async(lba, b.data()));
      if (kind == 3) m.live.erase(lba); else m.live[lba] = b;
    }
    for (auto t : tickets) v->wait(t);
    if (round % 4 == 1) m.snaps[v->snapshot()] = m.live;
    if (round % 4 == 3 && m.snaps.size() > 2) {
      v->delete_snapshot(m.snaps.begin()->first);
      m.snaps.erase(m.snaps.begin());
    }
    if (round % 5 == 4) v->checkpoint();
    if (round % 7 == 6) {
      v.reset();
      v = Volume::open(env, o);
    }
    verify(*v, m, o.blocks);
  }
  Stats s = v->stats();
  CHECK(s.dedup_hits > 0);
  CHECK(s.zero_writes > 0);
  CHECK(s.user_bytes_appended < s.new_chunks * kBlockSize);  // LZ4 saved space
  CHECK(s.gc_runs > 0);
  CHECK(s.segments_freed > 0);
  CHECK(s.segments <= o.capacity_segments + 1);

  // Deleting every snapshot and trimming everything returns all space.
  for (auto& [id, snap] : m.snaps) v->delete_snapshot(id);
  for (uint64_t lba = 0; lba < o.blocks; lba++) v->trim(lba);
  v->check_invariants();
  CHECK(v->stats().live_chunks == 0);
  CHECK(v->snapshots().empty());
}

TEST_CASE("volume: posix env round trip with concurrent writers and group commit") {
  // CRC32C check value from RFC 3720.
  REQUIRE(crc32c("123456789", 9) == 0xE3069283u);

  std::string dir = temp_dir("blockstore-posix");
  Options o;
  o.blocks = 4096;
  o.segment_bytes = 1 << 20;
  const int threads = 8, per = 60;
  {
    auto env = posix_env(dir);
    auto v = Volume::open(*env, o);
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; t++)
      ts.emplace_back([&, t] {
        for (int i = 0; i < per; i++) {
          Block b = make_block(uint64_t(t) * 1000 + i, i % 2);
          v->write(uint64_t(t) * per + i, b.data());
        }
      });
    for (auto& t : ts) t.join();
    Stats s = v->stats();
    MESSAGE("io: " << v->describe() << ", " << s.writes << " writes in " << s.groups << " groups");
    CHECK(s.groups < s.writes);  // concurrent writers shared fsyncs
  }
  auto env = posix_env(dir);
  auto v = Volume::open(*env, o);
  CHECK(v->stats().recovered_groups > 0);
  Block got(kBlockSize);
  for (int t = 0; t < threads; t++)
    for (int i = 0; i < per; i++) {
      REQUIRE(v->read(uint64_t(t) * per + i, got.data()) == ReadStatus::Ok);
      REQUIRE(got == make_block(uint64_t(t) * 1000 + i, i % 2));
    }
  v.reset();
  rm_rf(dir);
}
