// blockstore_bench: the measurements quoted in the README.
//
//   randwrite  --dir D [--qd 32] [--seconds 15] [--blocks N] [--io auto|psync|uring] [--no-direct]
//   replay     --dir D [--mib 1024]
//   gc         --dir D [--utils 0.5,0.7,0.8] [--segment-mb 4] [--capacity 128] [--passes 3]
//   ingest     --dir D [--no-dedup] [--no-compress] FILE...
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <fstream>
#include <random>
#include <sstream>
#include <thread>

#include "blockstore/args.hpp"
#include "blockstore/volume.hpp"

using namespace blockstore;
using Clock = std::chrono::steady_clock;

namespace {

double secs(Clock::time_point a, Clock::time_point b) { return std::chrono::duration<double>(b - a).count(); }

PosixOptions posix_opts(const Args& a) {
  PosixOptions po;
  std::string io = a.str("io", "auto");
  po.io = io == "psync" ? IoMode::Psync : io == "uring" ? IoMode::Uring : IoMode::Auto;
  po.direct = !a.has("no-direct");
  po.preallocate = !a.has("no-prealloc");
  return po;
}

// Incompressible, never-repeating blocks: the counter in the first 16 bytes defeats dedup.
struct RandomBlocks {
  std::vector<uint8_t> pool;
  explicit RandomBlocks(uint64_t seed) : pool(256 * kBlockSize) {
    std::mt19937_64 r(seed);
    for (size_t i = 0; i < pool.size(); i += 8) {
      uint64_t x = r();
      std::memcpy(&pool[i], &x, 8);
    }
  }
  void fill(uint8_t* out, uint64_t a, uint64_t b) const {
    std::memcpy(out, &pool[(b % 256) * kBlockSize], kBlockSize);
    std::memcpy(out, &a, 8);
    std::memcpy(out + 8, &b, 8);
  }
};

// Fill [0, blocks) or random lbas from `threads` threads with async windows; returns seconds.
double write_load(Volume& v, uint64_t blocks, uint64_t ops, bool sequential, int threads, uint64_t seed) {
  std::atomic<uint64_t> next{0};
  auto t0 = Clock::now();
  std::vector<std::thread> ts;
  for (int t = 0; t < threads; t++)
    ts.emplace_back([&, t] {
      RandomBlocks rb(seed + t);
      std::mt19937_64 r(seed * 31 + t);
      uint8_t buf[kBlockSize];
      std::vector<uint64_t> tickets;
      for (;;) {
        uint64_t start = next.fetch_add(64);
        if (start >= ops) break;
        tickets.clear();
        for (uint64_t i = start; i < std::min(ops, start + 64); i++) {
          rb.fill(buf, seed + t, i);
          tickets.push_back(v.write_async(sequential ? i % blocks : r() % blocks, buf));
        }
        for (auto tk : tickets) v.wait(tk);
      }
    });
  for (auto& t : ts) t.join();
  return secs(t0, Clock::now());
}

int randwrite(const Args& a) {
  auto env = posix_env(a.str("dir"), posix_opts(a));
  Options o;
  o.blocks = a.u64("blocks", 1 << 20);  // 4 GiB logical
  o.segment_bytes = a.u64("segment-mb", 64) << 20;
  // Like fio's layout phase: zero-fill segment files before the timed run so it
  // measures steady-state commits, not first-touch extent allocation.
  o.spare_segments = uint32_t(a.u64("prepare-segments", 0));
  int qd = int(a.u64("qd", 32));
  double seconds = a.f64("seconds", 15), warmup = a.f64("warmup", 2);
  auto v = Volume::open(*env, o);
  auto tp = Clock::now();
  while (v->stats().segments_zeroed < o.spare_segments) std::this_thread::sleep_for(std::chrono::milliseconds(20));
  if (o.spare_segments)
    std::printf("prepared %u x %llu MiB segments in %.1fs\n", o.spare_segments,
                (unsigned long long)(o.segment_bytes >> 20), secs(tp, Clock::now()));
  std::this_thread::sleep_for(std::chrono::duration<double>(a.f64("settle", 0)));
  std::atomic<bool> measuring{false}, stop{false};
  std::vector<std::vector<uint32_t>> lat(qd);  // microseconds
  std::vector<std::thread> ts;
  for (int t = 0; t < qd; t++)
    ts.emplace_back([&, t] {
      RandomBlocks rb(t);
      std::mt19937_64 r(t * 7919);
      uint8_t buf[kBlockSize];
      for (uint64_t i = 0; !stop; i++) {
        rb.fill(buf, t, i);
        auto s = Clock::now();
        v->write(r() % o.blocks, buf);
        if (measuring)
          lat[t].push_back(uint32_t(std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - s).count()));
      }
    });
  std::this_thread::sleep_for(std::chrono::duration<double>(warmup));
  Stats s0 = v->stats();
  measuring = true;
  auto t0 = Clock::now();
  std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
  measuring = false;
  double el = secs(t0, Clock::now());
  Stats s1 = v->stats();
  stop = true;
  for (auto& t : ts) t.join();
  std::vector<uint32_t> all;
  for (auto& l : lat) all.insert(all.end(), l.begin(), l.end());
  std::sort(all.begin(), all.end());
  auto pct = [&](double p) { return all.empty() ? 0.0 : all[size_t(p * (all.size() - 1))] / 1000.0; };
  double iops = all.size() / el;
  std::printf("randwrite 4KiB qd=%d io=%s\n", qd, v->describe().c_str());
  std::printf("  %.0f IOPS (%.1f MiB/s), latency ms p50 %.3f p99 %.3f p99.9 %.3f, %.1f writes/group\n", iops,
              iops * kBlockSize / (1 << 20), pct(0.5), pct(0.99), pct(0.999),
              double(s1.writes - s0.writes) / double(std::max<uint64_t>(1, s1.groups - s0.groups)));
  return 0;
}

int replay(const Args& a) {
  std::string dir = a.str("dir");
  uint64_t mib = a.u64("mib", 1024);
  Options o;
  o.blocks = mib * 256;
  o.checkpoint_bytes = 0;  // keep the whole history in the journal
  o.compress = false;
  {
    auto env = posix_env(dir, posix_opts(a));
    auto v = Volume::open(*env, o);
    double t = write_load(*v, o.blocks, o.blocks, true, 8, 1);
    Stats s = v->stats();
    std::printf("wrote %llu MiB in %.1fs (%llu groups, journal %.1f MiB)\n", (unsigned long long)mib, t,
                (unsigned long long)s.groups, s.journal_bytes / 1048576.0);
  }
  {
    auto env = posix_env(dir, posix_opts(a));
    auto v = Volume::open(*env, o);
    Stats s = v->stats();
    std::printf("reopen, journal replay: %.0f ms (%llu groups, %llu records)\n", s.recovery_ms,
                (unsigned long long)s.recovered_groups, (unsigned long long)s.recovered_records);
    v->checkpoint();
  }
  auto env = posix_env(dir, posix_opts(a));
  auto v = Volume::open(*env, o);
  std::printf("reopen, from checkpoint: %.0f ms\n", v->stats().recovery_ms);
  return 0;
}

int gc(const Args& a) {
  std::string base = a.str("dir");
  std::stringstream utils(a.str("utils", "0.5,0.6,0.7,0.8,0.9"));
  Options o;
  o.segment_bytes = a.u64("segment-mb", 4) << 20;
  o.capacity_segments = uint32_t(a.u64("capacity", 128));
  o.compress = false;  // fixed-size chunks so utilisation is exact
  o.checkpoint_bytes = 64 << 20;
  double passes = a.f64("passes", 3);
  std::printf("GC write amplification, uniform random 4KiB overwrites, greedy victim, %u x %llu MiB segments\n",
              o.capacity_segments, (unsigned long long)(o.segment_bytes >> 20));
  std::printf("  %-6s %-12s %-10s %-8s\n", "util", "overwrites", "gc MiB", "WA");
  for (std::string u; std::getline(utils, u, ',');) {
    double util = std::stod(u);
    std::string dir = base + "/util-" + u;
    o.blocks = uint64_t(util * double(o.capacity_segments * o.segment_bytes) / kBlockSize);
    auto env = posix_env(dir, posix_opts(a));
    auto v = Volume::open(*env, o);
    write_load(*v, o.blocks, o.blocks, true, 4, 1);
    write_load(*v, o.blocks, o.blocks, false, 4, 2);  // reach steady state before measuring
    Stats s0 = v->stats();
    uint64_t ops = uint64_t(passes * double(o.blocks));
    write_load(*v, o.blocks, ops, false, 4, 3);
    Stats s1 = v->stats();
    double user = double(s1.user_bytes_appended - s0.user_bytes_appended);
    double gcb = double(s1.gc_bytes_appended - s0.gc_bytes_appended);
    std::printf("  %-6.2f %-12llu %-10.0f %-8.2f\n", util, (unsigned long long)ops, gcb / 1048576.0,
                (user + gcb) / user);
    std::fflush(stdout);
  }
  return 0;
}

int ingest(const Args& a) {
  std::vector<std::string> files(a.positional.begin() + 1, a.positional.end());
  uint64_t total = 0;
  for (auto& f : files) {
    std::ifstream in(f, std::ios::binary | std::ios::ate);
    if (!in) {
      std::fprintf(stderr, "cannot open %s\n", f.c_str());
      return 1;
    }
    total += (uint64_t(in.tellg()) + kBlockSize - 1) / kBlockSize;
  }
  Options o;
  o.blocks = total;
  o.dedup = !a.has("no-dedup");
  o.compress = !a.has("no-compress");
  auto env = posix_env(a.str("dir"), posix_opts(a));
  auto v = Volume::open(*env, o);
  auto t0 = Clock::now();
  uint64_t lba = 0;
  std::vector<uint8_t> buf(256 * kBlockSize);
  for (auto& f : files) {
    std::ifstream in(f, std::ios::binary);
    while (in) {
      std::fill(buf.begin(), buf.end(), 0);
      in.read(reinterpret_cast<char*>(buf.data()), std::streamsize(buf.size()));
      size_t n = (size_t(in.gcount()) + kBlockSize - 1) / kBlockSize;
      uint64_t last = 0;
      for (size_t i = 0; i < n; i++) last = v->write_async(lba++, &buf[i * kBlockSize]);
      if (last) v->wait(last);
    }
  }
  double el = secs(t0, Clock::now());
  Stats s = v->stats();
  double logical = double(total) * kBlockSize, stored = double(s.user_bytes_appended);
  uint64_t nonzero = s.writes - s.zero_writes;
  std::printf("ingest %zu file(s), %.1f MiB logical in %.1fs, dedup=%d lz4=%d\n", files.size(), logical / 1048576.0, el,
              o.dedup, o.compress);
  std::printf("  zero blocks %llu (%.1f%%), non-zero %llu, dedup hits %llu, unique chunks %llu\n",
              (unsigned long long)s.zero_writes, 100.0 * s.zero_writes / s.writes, (unsigned long long)nonzero,
              (unsigned long long)s.dedup_hits, (unsigned long long)s.new_chunks);
  std::printf("  stored %.1f MiB: logical/stored %.2fx, non-zero/stored %.2fx (dedup %.2fx, lz4 %.2fx)\n",
              stored / 1048576.0, logical / stored, double(nonzero) * kBlockSize / stored,
              double(nonzero) / double(std::max<uint64_t>(1, s.new_chunks)),
              double(s.new_chunks) * kBlockSize / stored);
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Args a(argc, argv, {"no-direct", "no-prealloc", "no-compress", "no-dedup"});
  std::string mode = a.positional.empty() ? "" : a.positional[0];
  if (!a.has("dir") || mode.empty()) {
    std::fprintf(stderr, "usage: blockstore_bench randwrite|replay|gc|ingest --dir D [options] (see source header)\n");
    return 2;
  }
  try {
    if (mode == "randwrite") return randwrite(a);
    if (mode == "replay") return replay(a);
    if (mode == "gc") return gc(a);
    if (mode == "ingest") return ingest(a);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return 1;
  }
  std::fprintf(stderr, "unknown mode %s\n", mode.c_str());
  return 2;
}
