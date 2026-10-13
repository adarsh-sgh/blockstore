// Power-loss simulation: writers run against a SimEnv, the "disk" freezes at a
// random moment, unsynced writes are dropped/torn/garbled, and the volume must
// recover with every acknowledged write intact.
#include <atomic>
#include <chrono>
#include <thread>

#include "doctest.h"
#include "util.hpp"

using namespace blockstore;
using namespace testutil;

namespace {
struct History {
  Block base = Block(kBlockSize, 0);  // value as of the last recovery
  std::vector<Block> issued;          // writes since then, in staging order
  long acked = -1;                    // highest acknowledged index into issued
};
}  // namespace

TEST_CASE("crash: power loss at random points never loses an acknowledged write") {
  const uint64_t iters = env_u64("BLOCKSTORE_CRASH_ITERS", 200);
  SimEnv env;
  Options o;
  o.blocks = 512;
  o.segment_bytes = 32 << 10;
  o.capacity_segments = 96;  // 3 MiB for 2 MiB logical + snapshots: GC under pressure
  o.checkpoint_bytes = 64 << 10;
  std::vector<History> h(o.blocks);
  std::mt19937_64 rng(7);
  constexpr int kThreads = 4;  // thread t owns lbas with lba % kThreads == t
  uint64_t acked_total = 0, dropped = 0, checkpoints = 0, gc_runs = 0;

  for (uint64_t iter = 0; iter < iters; iter++) {
    auto v = Volume::open(env, o);
    dropped += v->stats().dropped_groups;
    Block got(kBlockSize);
    for (uint64_t lba = 0; lba < o.blocks; lba++) {
      REQUIRE(v->read(lba, got.data()) == ReadStatus::Ok);
      History& x = h[lba];
      bool allowed = x.acked < 0 && got == x.base;
      for (size_t k = x.acked < 0 ? 0 : size_t(x.acked); k < x.issued.size() && !allowed; k++)
        allowed = got == x.issued[k];
      REQUIRE_MESSAGE(allowed, "iter " << iter << " lba " << lba << " lost an acknowledged write");
      x = History{got, {}, -1};
    }
    for (uint32_t id : v->snapshots())
      for (uint64_t lba = 0; lba < o.blocks; lba++) REQUIRE(v->read(lba, got.data(), id) == ReadStatus::Ok);
    v->check_invariants();

    std::atomic<uint64_t> acked{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < kThreads; t++)
      ts.emplace_back([&, t, seed = rng()] {
        std::mt19937_64 r(seed);
        try {
          for (uint64_t n = 0;; n++) {
            // A window of async writes, so groups carry several records.
            std::vector<std::pair<uint64_t, long>> win;
            std::vector<uint64_t> tickets;
            for (size_t w = 1 + r() % 8; w; w--) {
              uint64_t lba = (r() % (o.blocks / kThreads)) * kThreads + t;
              Block b = make_block(r(), pick_kind(r), n);
              h[lba].issued.push_back(b);
              try {
                tickets.push_back(v->write_async(lba, b.data()));
              } catch (const NoSpace&) {
                continue;
              }
              win.emplace_back(lba, long(h[lba].issued.size()) - 1);
            }
            for (auto tk : tickets) v->wait(tk);
            for (auto [lba, k] : win) h[lba].acked = std::max(h[lba].acked, k);
            acked += win.size();
            if (t == 0 && r() % 50 == 0) {
              auto snaps = v->snapshots();
              if (snaps.size() >= 2) v->delete_snapshot(snaps.front());
              else v->snapshot();
            }
          }
        } catch (const IoError&) {  // the crash
        }
      });
    // Crash after a random number of disk operations (lands inside group
    // commits, checkpoints and GC), or after a random time, whichever is first.
    env.freeze_after(1 + rng() % 400);
    std::this_thread::sleep_for(std::chrono::microseconds(500 + rng() % 20000));
    env.freeze();
    for (auto& t : ts) t.join();
    auto s = v->stats();
    checkpoints += s.checkpoints;
    gc_runs += s.gc_runs;
    v.reset();
    env.crash(rng);
    acked_total += acked;
  }
  MESSAGE(iters << " crashes, " << acked_total << " acknowledged writes, 0 lost; " << dropped
                << " torn groups dropped, " << checkpoints << " checkpoints, " << gc_runs << " GC runs");
  CHECK(dropped > 0);  // the torn-tail path was exercised
  CHECK(checkpoints > 0);
  CHECK(gc_runs > 0);
}
