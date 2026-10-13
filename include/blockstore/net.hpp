#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace blockstore::net {

int connect_tcp(const std::string& host, uint16_t port);  // -1 on failure
bool read_full(int fd, void* p, size_t n);
bool write_full(int fd, const void* p, size_t n);
uint64_t be64(uint64_t v);
uint32_t be32(uint32_t v);
uint16_t be16(uint16_t v);

// Accept loop on its own thread; one thread per connection, all joined on stop().
class Listener {
 public:
  Listener(const std::string& host, uint16_t port, std::function<void(int)> serve);
  ~Listener();
  uint16_t port() const { return port_; }
  void stop();

 private:
  void loop();
  int fd_ = -1;
  uint16_t port_ = 0;
  std::function<void(int)> serve_;
  std::atomic<bool> stop_{false};
  std::mutex mu_;
  std::vector<int> conns_;
  std::vector<std::thread> threads_;
  std::thread accept_;
};

}  // namespace blockstore::net
