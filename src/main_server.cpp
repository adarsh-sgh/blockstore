// blockstored: serve a volume over NBD, optionally replicating synchronously to a standby.
//
//   blockstored --dir D [--nbd-port 10809] [--replicate-to host:port]
//   blockstored --dir D --standby-port 10810 --nbd-port 10809   # SIGUSR1 promotes it
#include <signal.h>

#include <cstdio>
#include <iostream>
#include <memory>

#include "blockstore/args.hpp"
#include "blockstore/nbd.hpp"
#include "blockstore/replication.hpp"

using namespace blockstore;

int main(int argc, char** argv) {
  Args a(argc, argv, {"no-direct", "no-prealloc", "no-compress", "no-dedup"});
  if (!a.has("dir")) {
    std::fprintf(stderr,
                 "usage: blockstored --dir D [--blocks N] [--segment-mb M] [--capacity-segments C]\n"
                 "         [--checkpoint-mb K] [--no-compress] [--no-dedup] [--io auto|psync|uring] [--no-direct]\n"
                 "         [--host 0.0.0.0] [--nbd-port P] [--replicate-to host:port] [--standby-port P]\n");
    return 2;
  }
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGINT), sigaddset(&set, SIGTERM), sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, nullptr);  // handled by sigwait below, in every thread
  signal(SIGPIPE, SIG_IGN);

  PosixOptions po;
  std::string io = a.str("io", "auto");
  po.io = io == "psync" ? IoMode::Psync : io == "uring" ? IoMode::Uring : IoMode::Auto;
  po.direct = !a.has("no-direct");
  Options o;
  o.blocks = a.u64("blocks", 1 << 20);
  o.segment_bytes = a.u64("segment-mb", 64) << 20;
  o.capacity_segments = static_cast<uint32_t>(a.u64("capacity-segments", 0));
  o.checkpoint_bytes = a.u64("checkpoint-mb", 256) << 20;
  o.compress = !a.has("no-compress");
  o.dedup = !a.has("no-dedup");
  std::string host = a.str("host", "0.0.0.0");
  auto nbd_port = static_cast<uint16_t>(a.u64("nbd-port", 10809));

  try {
    auto env = posix_env(a.str("dir"), po);
    auto vol = Volume::open(*env, o);
    auto st = vol->stats();
    std::printf("blockstored: %llu blocks, io %s, recovered %llu groups in %.1f ms (%llu dropped)\n",
                (unsigned long long)vol->blocks(), vol->describe().c_str(),
                (unsigned long long)st.recovered_groups, st.recovery_ms, (unsigned long long)st.dropped_groups);

    std::unique_ptr<ReplicaLink> link;
    if (a.has("replicate-to")) {
      std::string t = a.str("replicate-to");
      auto colon = t.rfind(':');
      link = std::make_unique<ReplicaLink>(t.substr(0, colon), static_cast<uint16_t>(std::stoi(t.substr(colon + 1))));
      vol->set_sink(link.get());
      std::printf("replicating to %s\n", t.c_str());
    }
    std::unique_ptr<StandbyServer> standby;
    std::unique_ptr<NbdServer> nbd;
    if (a.has("standby-port")) {
      standby = std::make_unique<StandbyServer>(*vol, host, static_cast<uint16_t>(a.u64("standby-port", 0)));
      std::printf("standby listening on %u\n", standby->port());
    } else {
      nbd = std::make_unique<NbdServer>(*vol, host, nbd_port);
      std::printf("nbd listening on %u\n", nbd->port());
    }
    std::fflush(stdout);

    for (;;) {
      int sig = 0;
      sigwait(&set, &sig);
      if (sig == SIGUSR1 && standby) {
        standby->stop();  // no more groups from the old primary
        standby.reset();
        nbd = std::make_unique<NbdServer>(*vol, host, nbd_port);
        std::printf("promoted; nbd listening on %u\n", nbd->port());
        std::fflush(stdout);
        continue;
      }
      if (sig == SIGINT || sig == SIGTERM) break;
    }
    if (nbd) nbd->stop();
    if (standby) standby->stop();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "blockstored: %s\n", e.what());
    return 1;
  }
  return 0;
}
