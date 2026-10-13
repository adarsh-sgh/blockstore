#include "blockstore/replication.hpp"

#include <unistd.h>

#include <chrono>
#include <cstring>
#include <thread>

#include "blockstore/crc32c.hpp"

namespace blockstore {

namespace {
constexpr uint32_t kReplMagic = 0x31525342;  // "BSR1"
constexpr uint32_t kMaxBatch = 256u << 20;
#pragma pack(push, 1)
struct Header {
  uint32_t magic, crc;
  uint64_t seq;
  uint32_t len;
};
struct Ack {
  uint32_t magic, ok;
  uint64_t seq;
};
#pragma pack(pop)
}  // namespace

StandbyServer::StandbyServer(Volume& v, const std::string& host, uint16_t port) : v_(v) {
  listener_ = std::make_unique<net::Listener>(host, port, [this](int fd) { serve(fd); });
}

void StandbyServer::serve(int fd) {
  std::vector<uint8_t> buf;
  for (;;) {
    Header h;
    if (!net::read_full(fd, &h, sizeof h) || h.magic != kReplMagic || h.len > kMaxBatch) return;
    buf.resize(h.len);
    if (!net::read_full(fd, buf.data(), h.len) || crc32c(buf.data(), h.len) != h.crc) return;
    Ack a{kReplMagic, 1, h.seq};
    try {
      v_.apply_replicated(buf.data(), buf.size());
    } catch (const std::exception&) {
      a.ok = 0;
    }
    if (!net::write_full(fd, &a, sizeof a) || !a.ok) return;
  }
}

ReplicaLink::ReplicaLink(const std::string& host, uint16_t port, int connect_timeout_ms) {
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(connect_timeout_ms);
  while ((fd_ = net::connect_tcp(host, port)) < 0) {
    if (std::chrono::steady_clock::now() > deadline) throw IoError("standby unreachable");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
}

ReplicaLink::~ReplicaLink() {
  if (fd_ >= 0) ::close(fd_);
}

void ReplicaLink::send(uint64_t seq, const std::vector<uint8_t>& ops) {
  Header h{kReplMagic, crc32c(ops.data(), ops.size()), seq, static_cast<uint32_t>(ops.size())};
  if (!net::write_full(fd_, &h, sizeof h) || !net::write_full(fd_, ops.data(), ops.size()))
    throw IoError("standby lost (send)");
}

void ReplicaLink::wait_ack(uint64_t seq) {
  Ack a;
  if (!net::read_full(fd_, &a, sizeof a) || a.magic != kReplMagic || a.seq != seq || !a.ok)
    throw IoError("standby lost (ack)");
}

}  // namespace blockstore
