// Fault injection: media bit flips (in-process), kill -9 of the server and
// failover to the standby (real blockstored processes, driven over NBD).
#include <atomic>
#include <chrono>
#include <map>
#include <thread>
#include <unordered_map>

#include "blockstore/nbd.hpp"
#include "doctest.h"
#include "proc.hpp"
#include "util.hpp"

using namespace blockstore;
using namespace testutil;

namespace {

struct History {
  Block base = Block(kBlockSize, 0);
  std::vector<Block> issued;
  long acked = -1;
};

struct Tracker {
  std::map<uint64_t, History> h;
  uint64_t acked = 0, failed = 0;

  // Pipelined single-block writes at queue depth `qd` until `stop` or the connection dies.
  void write_until(NbdClient& c, std::mt19937_64& r, uint64_t lbas, size_t qd, const std::atomic<bool>& stop,
                   uint64_t max_ops = UINT64_MAX) {
    std::unordered_map<uint64_t, std::pair<uint64_t, long>> inflight;
    uint64_t handle = 0, sent = 0;
    for (;;) {
      while (inflight.size() < qd && !stop && sent < max_ops) {
        uint64_t lba = r() % lbas;
        Block b = make_block(r(), pick_kind(r), ++handle);
        auto& x = h[lba];
        x.issued.push_back(b);
        if (!c.send_write(handle, lba * kBlockSize, b.data(), kBlockSize)) return;
        inflight[handle] = {lba, long(x.issued.size()) - 1};
        sent++;
      }
      if (inflight.empty()) return;
      NbdClient::Reply rep;
      if (!c.recv(rep)) return;
      auto it = inflight.find(rep.handle);
      REQUIRE(it != inflight.end());
      if (rep.error) {
        failed++;
      } else {
        auto& x = h[it->second.first];
        x.acked = std::max(x.acked, it->second.second);
        acked++;
      }
      inflight.erase(it);
    }
  }

  // Every acknowledged write must be readable: the value is the last acked
  // version or a later (unacknowledged) one.
  void verify(NbdClient& c) {
    std::vector<uint64_t> lbas;
    for (auto& [lba, x] : h) lbas.push_back(lba);
    for (size_t i = 0; i < lbas.size(); i += 64) {
      size_t n = std::min<size_t>(64, lbas.size() - i);
      for (size_t k = 0; k < n; k++) REQUIRE(c.send_read(lbas[i + k], lbas[i + k] * kBlockSize, kBlockSize));
      for (size_t k = 0; k < n; k++) {
        NbdClient::Reply rep;
        REQUIRE(c.recv(rep));
        REQUIRE(rep.error == 0);
        History& x = h[rep.handle];
        bool ok = x.acked < 0 && rep.data == x.base;
        for (size_t j = x.acked < 0 ? 0 : size_t(x.acked); j < x.issued.size() && !ok; j++)
          ok = rep.data == x.issued[j];
        REQUIRE_MESSAGE(ok, "lba " << rep.handle << " lost an acknowledged write");
        x = History{rep.data, {}, -1};
      }
    }
  }
};

std::vector<std::string> server_args(const std::string& dir) {
  return {BLOCKSTORED_PATH, "--dir", dir, "--host", "127.0.0.1", "--nbd-port", "0", "--blocks", "4096",
          "--segment-mb", "1", "--capacity-segments", "40", "--checkpoint-mb", "1"};
}

void kill_after(Proc& p, std::mt19937_64& r, std::atomic<bool>& stop, std::thread& t) {
  int ms = 30 + int(r() % 300);
  pid_t pid = p.pid;
  t = std::thread([ms, pid, &stop] {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    ::kill(pid, SIGKILL);
    stop = true;
  });
}

}  // namespace

TEST_CASE("faults: bit flips in data, journal and checkpoint are detected, never returned as data") {
  std::mt19937_64 rng(99);
  Options o;
  o.blocks = 1024;
  o.segment_bytes = 256 << 10;
  o.checkpoint_bytes = 0;

  // Data: flip bits all over the segment files; every read is either correct or Corrupt.
  SimEnv env;
  std::map<uint64_t, Block> model;
  {
    auto v = Volume::open(env, o);
    for (uint64_t lba = 0; lba < o.blocks; lba += 16) {
      std::vector<uint64_t> ts;
      for (uint64_t i = lba; i < lba + 16; i++) {
        model[i] = make_block(rng(), pick_kind(rng), i);
        ts.push_back(v->write_async(i, model[i].data()));
      }
      for (auto t : ts) v->wait(t);
    }
    v->checkpoint();  // nothing left for open() to validate, so flips stay in place
  }
  uint64_t flips = 0;
  for (auto& name : env.list()) {
    if (name.rfind("seg-", 0) != 0) continue;
    uint64_t size = env.open(name, false)->size();
    for (int i = 0; i < 40; i++) flips += env.flip_bit(name, rng() % size, uint8_t(1u << (rng() % 8)));
  }
  auto v = Volume::open(env, o);
  uint64_t corrupt = 0;
  Block got(kBlockSize);
  for (auto& kv : model) {
    uint64_t lba = kv.first;
    const Block& want = kv.second;
    if (v->read(lba, got.data()) == ReadStatus::Corrupt) {
      corrupt++;
      continue;
    }
    REQUIRE_MESSAGE(got == want, "silent corruption at lba " << lba);
  }
  uint64_t scrubbed = v->scrub();
  MESSAGE(flips << " bit flips in segments: " << corrupt << " reads failed their crc32c, 0 returned bad data; scrub found "
                << scrubbed << " corrupt chunks");
  CHECK(corrupt > 0);
  CHECK(scrubbed > 0);
  v.reset();

  // Journal: a flip in the middle (valid frames follow it) refuses to open
  // instead of silently truncating acknowledged history.
  SimEnv jenv;
  uint64_t jbytes;
  {
    auto w = Volume::open(jenv, o);
    for (uint64_t i = 0; i < 200; i++) w->write(i, make_block(i, 1).data());
    jbytes = w->stats().journal_bytes;
  }
  std::string journal;
  for (auto& n : jenv.list())
    if (n.rfind("journal-", 0) == 0) journal = n;
  REQUIRE(jenv.flip_bit(journal, jbytes / 3, 0x10));
  CHECK_THROWS_AS(Volume::open(jenv, o), Corruption);

  // Checkpoint: whole-image crc32c.
  SimEnv cenv;
  { auto w = Volume::open(cenv, o); w->write(1, make_block(1, 0).data()); w->checkpoint(); }
  REQUIRE(cenv.flip_bit("checkpoint", 40, 0x01));
  CHECK_THROWS_AS(Volume::open(cenv, o), Corruption);
}

TEST_CASE("faults: kill -9 of the server never loses an acknowledged write") {
  const uint64_t cycles = env_u64("BLOCKSTORE_KILL_CYCLES", 12);
  std::string dir = temp_dir("blockstore-kill");
  std::mt19937_64 rng(5);
  Tracker tr;
  for (uint64_t cycle = 0; cycle < cycles; cycle++) {
    Proc p = Proc::spawn(server_args(dir));
    NbdClient c("127.0.0.1", static_cast<uint16_t>(p.wait_port("nbd listening")));
    tr.verify(c);  // everything acknowledged before the previous kill survived
    std::atomic<bool> stop{false};
    std::thread killer;
    kill_after(p, rng, stop, killer);
    tr.write_until(c, rng, 4096, 32, stop);
    killer.join();
    p.wait();
  }
  Proc p = Proc::spawn(server_args(dir));
  NbdClient c("127.0.0.1", static_cast<uint16_t>(p.wait_port("nbd listening")));
  tr.verify(c);
  MESSAGE(cycles << " kill -9 cycles: " << tr.acked << " acknowledged writes, 0 lost");
  CHECK(tr.acked > 0);
  c.disconnect();
  p.signal(SIGTERM);
  CHECK(p.wait() == 0);
  rm_rf(dir);
}

TEST_CASE("faults: failover to the standby loses no acknowledged write") {
  const uint64_t cycles = env_u64("BLOCKSTORE_FAILOVER_CYCLES", 8);
  std::mt19937_64 rng(11);
  uint64_t acked = 0;
  for (uint64_t cycle = 0; cycle < cycles; cycle++) {
    std::string pdir = temp_dir("blockstore-primary"), sdir = temp_dir("blockstore-standby");
    auto sargs = server_args(sdir);
    sargs.insert(sargs.end(), {"--standby-port", "0"});
    Proc standby = Proc::spawn(sargs);
    int sport = standby.wait_port("standby listening");
    auto pargs = server_args(pdir);
    pargs.insert(pargs.end(), {"--replicate-to", "127.0.0.1:" + std::to_string(sport)});
    Proc primary = Proc::spawn(pargs);
    Tracker tr;
    {
      NbdClient c("127.0.0.1", static_cast<uint16_t>(primary.wait_port("nbd listening")));
      std::atomic<bool> stop{false};
      std::thread killer;
      kill_after(primary, rng, stop, killer);
      tr.write_until(c, rng, 4096, 32, stop);
      killer.join();
      primary.wait();
    }
    standby.signal(SIGUSR1);
    NbdClient c("127.0.0.1", static_cast<uint16_t>(standby.wait_port("promoted; nbd listening")));
    tr.verify(c);
    acked += tr.acked;
    std::atomic<bool> never{false};
    tr.write_until(c, rng, 4096, 8, never, 200);  // the promoted standby takes writes
    tr.verify(c);
    c.disconnect();
    standby.signal(SIGTERM);
    CHECK(standby.wait() == 0);
    rm_rf(pdir), rm_rf(sdir);
  }
  MESSAGE(cycles << " failovers after kill -9 of the primary: " << acked << " acknowledged writes, 0 lost");
  CHECK(acked > 0);
}

TEST_CASE("faults: losing the standby fails writes instead of acknowledging them") {
  std::string pdir = temp_dir("blockstore-primary"), sdir = temp_dir("blockstore-standby");
  auto sargs = server_args(sdir);
  sargs.insert(sargs.end(), {"--standby-port", "0"});
  Proc standby = Proc::spawn(sargs);
  auto pargs = server_args(pdir);
  pargs.insert(pargs.end(), {"--replicate-to", "127.0.0.1:" + std::to_string(standby.wait_port("standby listening"))});
  Proc primary = Proc::spawn(pargs);
  NbdClient c("127.0.0.1", static_cast<uint16_t>(primary.wait_port("nbd listening")));
  std::mt19937_64 rng(3);
  Tracker tr;
  std::atomic<bool> never{false};
  tr.write_until(c, rng, 4096, 8, never, 100);
  CHECK(tr.acked == 100);
  standby.kill9();
  uint64_t before = tr.acked;
  tr.write_until(c, rng, 4096, 8, never, 50);
  CHECK(tr.acked == before);  // nothing acknowledged without the standby
  CHECK(tr.failed == 50);
  c.disconnect();
  primary.kill9();
  rm_rf(pdir), rm_rf(sdir);
}
