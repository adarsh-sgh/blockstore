#include "blockstore/net.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace blockstore::net {

#ifdef MSG_NOSIGNAL
constexpr int kSendFlags = MSG_NOSIGNAL;
#else
constexpr int kSendFlags = 0;
#endif

namespace {
void tune(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
#ifdef SO_NOSIGPIPE
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof one);
#endif
}
}  // namespace

uint64_t be64(uint64_t v) { return __builtin_bswap64(v); }
uint32_t be32(uint32_t v) { return __builtin_bswap32(v); }
uint16_t be16(uint16_t v) { return __builtin_bswap16(v); }

int connect_tcp(const std::string& host, uint16_t port) {
  addrinfo hints{}, *res = nullptr;
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0) return -1;
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd >= 0 && ::connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
    ::close(fd);
    fd = -1;
  }
  ::freeaddrinfo(res);
  if (fd >= 0) tune(fd);
  return fd;
}

bool read_full(int fd, void* p, size_t n) {
  auto c = static_cast<char*>(p);
  while (n) {
    ssize_t r = ::recv(fd, c, n, 0);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    c += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

bool write_full(int fd, const void* p, size_t n) {
  auto c = static_cast<const char*>(p);
  while (n) {
    ssize_t r = ::send(fd, c, n, kSendFlags);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    c += r;
    n -= static_cast<size_t>(r);
  }
  return true;
}

Listener::Listener(const std::string& host, uint16_t port, std::function<void(int)> serve)
    : serve_(std::move(serve)) {
  fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  int one = 1;
  ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_port = htons(port);
  if (::inet_pton(AF_INET, host.c_str(), &a.sin_addr) != 1) throw std::runtime_error("bad host " + host);
  if (::bind(fd_, reinterpret_cast<sockaddr*>(&a), sizeof a) != 0 || ::listen(fd_, 64) != 0)
    throw std::runtime_error(std::string("listen: ") + std::strerror(errno));
  socklen_t len = sizeof a;
  ::getsockname(fd_, reinterpret_cast<sockaddr*>(&a), &len);
  port_ = ntohs(a.sin_port);
  accept_ = std::thread(&Listener::loop, this);
}

Listener::~Listener() { stop(); }

void Listener::loop() {
  while (!stop_) {
    pollfd p{fd_, POLLIN, 0};
    if (::poll(&p, 1, 100) <= 0) continue;
    int c = ::accept(fd_, nullptr, nullptr);
    if (c < 0) continue;
    tune(c);
    std::lock_guard<std::mutex> g(mu_);
    if (stop_) {
      ::close(c);
      break;
    }
    conns_.push_back(c);
    threads_.emplace_back([this, c] { serve_(c); });
  }
}

void Listener::stop() {
  if (stop_.exchange(true)) return;
  if (accept_.joinable()) accept_.join();
  {
    std::lock_guard<std::mutex> g(mu_);
    for (int c : conns_) ::shutdown(c, SHUT_RDWR);  // unblock readers
  }
  for (auto& t : threads_) t.join();
  for (int c : conns_) ::close(c);
  ::close(fd_);
}

}  // namespace blockstore::net
