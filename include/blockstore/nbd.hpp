#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "blockstore/net.hpp"
#include "blockstore/volume.hpp"

namespace blockstore {

// NBD (fixed newstyle, simple replies) export of one volume, so fio, qemu and
// nbd-client can drive it. Requests are pipelined: writes are staged as they
// arrive and replied to, in order, once their group commit is durable.
class NbdServer {
 public:
  NbdServer(Volume& v, const std::string& host, uint16_t port);
  uint16_t port() const { return listener_->port(); }
  void stop() { listener_->stop(); }

 private:
  void serve(int fd);
  bool handshake(int fd);
  Volume& v_;
  std::unique_ptr<net::Listener> listener_;
};

// Minimal pipelining NBD client (tests, load generator).
class NbdClient {
 public:
  NbdClient(const std::string& host, uint16_t port);  // throws on connect/handshake failure
  ~NbdClient();
  uint64_t size() const { return size_; }
  bool send_write(uint64_t handle, uint64_t off, const void* data, uint32_t len);
  bool send_read(uint64_t handle, uint64_t off, uint32_t len);
  bool send_flush(uint64_t handle);
  struct Reply {
    uint64_t handle = 0;
    uint32_t error = 0;
    std::vector<uint8_t> data;  // reads
  };
  bool recv(Reply& r);  // false when the connection is gone
  void disconnect();

 private:
  int fd_ = -1;
  uint64_t size_ = 0;
  std::unordered_map<uint64_t, uint32_t> read_len_;
};

}  // namespace blockstore
