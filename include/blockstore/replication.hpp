#pragma once
#include <memory>
#include <string>

#include "blockstore/net.hpp"
#include "blockstore/volume.hpp"

namespace blockstore {

// Standby side: applies each group a primary sends, acks once it is durable here.
class StandbyServer {
 public:
  StandbyServer(Volume& v, const std::string& host, uint16_t port);
  uint16_t port() const { return listener_->port(); }
  void stop() { listener_->stop(); }  // promotion: stop taking groups from the old primary

 private:
  void serve(int fd);
  Volume& v_;
  std::unique_ptr<net::Listener> listener_;
};

// Primary side: synchronous replication. A lost standby fails the group, so the
// primary never acknowledges a write the standby does not have.
class ReplicaLink : public GroupSink {
 public:
  ReplicaLink(const std::string& host, uint16_t port, int connect_timeout_ms = 5000);
  ~ReplicaLink() override;
  void send(uint64_t seq, const std::vector<uint8_t>& ops) override;
  void wait_ack(uint64_t seq) override;

 private:
  int fd_ = -1;
};

}  // namespace blockstore
