#include "blockstore/nbd.hpp"

#include <unistd.h>

#include <condition_variable>
#include <cstring>
#include <deque>
#include <mutex>
#include <stdexcept>

namespace blockstore {

using net::be16;
using net::be32;
using net::be64;
using net::read_full;
using net::write_full;

namespace {
constexpr uint64_t kNbdMagic = 0x4e42444d41474943;
constexpr uint64_t kOptMagic = 0x49484156454F5054;
constexpr uint64_t kRepMagic = 0x0003e889045565a9;
constexpr uint32_t kReqMagic = 0x25609513;
constexpr uint32_t kSimpleReplyMagic = 0x67446698;
constexpr uint16_t kFixedNewstyle = 1, kNoZeroes = 2;
constexpr uint32_t kOptExportName = 1, kOptAbort = 2, kOptInfo = 6, kOptGo = 7;
constexpr uint32_t kRepAck = 1, kRepInfo = 3, kRepErrUnsup = 0x80000001;
constexpr uint16_t kInfoExport = 0, kInfoBlockSize = 3;
constexpr uint16_t kTxFlags = 1 /*HAS_FLAGS*/ | 4 /*SEND_FLUSH*/ | 8 /*SEND_FUA*/ | 32 /*SEND_TRIM*/ |
                              64 /*SEND_WRITE_ZEROES*/;
enum : uint16_t { kCmdRead = 0, kCmdWrite = 1, kCmdDisc = 2, kCmdFlush = 3, kCmdTrim = 4, kCmdWriteZeroes = 6 };
constexpr uint32_t kEIO = 5, kENOMEM = 12, kEINVAL = 22, kENOSPC = 28;
constexpr uint32_t kMaxRequest = 32u << 20;

#pragma pack(push, 1)
struct Request {
  uint32_t magic;
  uint16_t flags, type;
  uint64_t handle, offset;
  uint32_t length;
};
struct SimpleReply {
  uint32_t magic, error;
  uint64_t handle;
};
#pragma pack(pop)
static_assert(sizeof(Request) == 28 && sizeof(SimpleReply) == 16, "wire layout");

bool send_opt_reply(int fd, uint32_t opt, uint32_t type, const std::vector<uint8_t>& data) {
  uint64_t m = be64(kRepMagic);
  uint32_t o = be32(opt), t = be32(type), l = be32(static_cast<uint32_t>(data.size()));
  return write_full(fd, &m, 8) && write_full(fd, &o, 4) && write_full(fd, &t, 4) && write_full(fd, &l, 4) &&
         (data.empty() || write_full(fd, data.data(), data.size()));
}

template <class T>
void put_be(std::vector<uint8_t>& v, T x) {
  if constexpr (sizeof(T) == 8) x = be64(x);
  if constexpr (sizeof(T) == 4) x = be32(x);
  if constexpr (sizeof(T) == 2) x = be16(x);
  size_t n = v.size();
  v.resize(n + sizeof(T));
  std::memcpy(v.data() + n, &x, sizeof(T));
}

// In-order reply queue: writes wait for their group commit before replying.
struct ReplyQueue {
  struct Item {
    uint64_t handle;
    uint32_t error;
    uint64_t ticket;  // 0 = nothing to wait for
    std::vector<uint8_t> data;
  };
  std::mutex mu;
  std::condition_variable cv;
  std::deque<Item> q;
  bool closed = false;
  void push(Item it) {
    std::lock_guard<std::mutex> g(mu);
    q.push_back(std::move(it));
    cv.notify_one();
  }
  bool pop(Item& it) {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return closed || !q.empty(); });
    if (q.empty()) return false;
    it = std::move(q.front());
    q.pop_front();
    return true;
  }
  void close() {
    std::lock_guard<std::mutex> g(mu);
    closed = true;
    cv.notify_all();
  }
};
}  // namespace

NbdServer::NbdServer(Volume& v, const std::string& host, uint16_t port) : v_(v) {
  listener_ = std::make_unique<net::Listener>(host, port, [this](int fd) { serve(fd); });
}

bool NbdServer::handshake(int fd) {
  uint64_t m1 = be64(kNbdMagic), m2 = be64(kOptMagic);
  uint16_t hf = be16(kFixedNewstyle | kNoZeroes);
  uint32_t cflags;
  if (!write_full(fd, &m1, 8) || !write_full(fd, &m2, 8) || !write_full(fd, &hf, 2) ||
      !read_full(fd, &cflags, 4))
    return false;
  bool no_zeroes = be32(cflags) & kNoZeroes;
  const uint64_t size = v_.blocks() * kBlockSize;
  for (;;) {
    uint64_t magic;
    uint32_t opt, len;
    if (!read_full(fd, &magic, 8) || !read_full(fd, &opt, 4) || !read_full(fd, &len, 4)) return false;
    opt = be32(opt), len = be32(len);
    if (be64(magic) != kOptMagic || len > 65536) return false;
    std::vector<uint8_t> data(len);
    if (len && !read_full(fd, data.data(), len)) return false;
    if (opt == kOptExportName) {
      uint64_t s = be64(size);
      uint16_t f = be16(kTxFlags);
      uint8_t zeros[124] = {};
      return write_full(fd, &s, 8) && write_full(fd, &f, 2) && (no_zeroes || write_full(fd, zeros, 124));
    }
    if (opt == kOptInfo || opt == kOptGo) {
      bool want_bs = false;
      if (len >= 6) {
        uint32_t nl;
        std::memcpy(&nl, data.data(), 4);
        nl = be32(nl);
        if (4ull + nl + 2 <= len) {
          uint16_t nreq;
          std::memcpy(&nreq, data.data() + 4 + nl, 2);
          nreq = be16(nreq);
          for (uint16_t i = 0; i < nreq && 4ull + nl + 2 + 2 * (i + 1) <= len; i++) {
            uint16_t r;
            std::memcpy(&r, data.data() + 4 + nl + 2 + 2 * i, 2);
            want_bs |= be16(r) == kInfoBlockSize;
          }
        }
      }
      std::vector<uint8_t> info;
      put_be<uint16_t>(info, kInfoExport), put_be<uint64_t>(info, size), put_be<uint16_t>(info, kTxFlags);
      if (!send_opt_reply(fd, opt, kRepInfo, info)) return false;
      if (want_bs) {
        std::vector<uint8_t> bs;
        put_be<uint16_t>(bs, kInfoBlockSize), put_be<uint32_t>(bs, kBlockSize), put_be<uint32_t>(bs, kBlockSize),
            put_be<uint32_t>(bs, kMaxRequest);
        if (!send_opt_reply(fd, opt, kRepInfo, bs)) return false;
      }
      if (!send_opt_reply(fd, opt, kRepAck, {})) return false;
      if (opt == kOptGo) return true;
      continue;
    }
    if (opt == kOptAbort) {
      send_opt_reply(fd, opt, kRepAck, {});
      return false;
    }
    if (!send_opt_reply(fd, opt, kRepErrUnsup, {})) return false;  // structured replies, meta contexts...
  }
}

void NbdServer::serve(int fd) {
  if (!handshake(fd)) return;
  ReplyQueue rq;
  std::thread replier([&] {
    ReplyQueue::Item it;
    bool ok = true;
    while (rq.pop(it)) {
      if (it.ticket && !it.error) {
        try {
          v_.wait(it.ticket);
        } catch (const std::exception&) {
          it.error = kEIO;
        }
      }
      SimpleReply r{be32(kSimpleReplyMagic), be32(it.error), it.handle};
      ok = ok && write_full(fd, &r, sizeof r) && (it.error || it.data.empty() || write_full(fd, it.data.data(), it.data.size()));
    }
  });
  static const uint8_t zero[kBlockSize] = {};
  std::vector<uint8_t> buf;
  for (;;) {
    Request q;
    if (!read_full(fd, &q, sizeof q) || be32(q.magic) != kReqMagic) break;
    uint16_t type = be16(q.type);
    uint64_t off = be64(q.offset);
    uint32_t len = be32(q.length);
    if (type == kCmdDisc) break;
    if (type == kCmdWrite) {
      if (len > kMaxRequest) break;
      buf.resize(len);
      if (!read_full(fd, buf.data(), len)) break;
    }
    ReplyQueue::Item it{q.handle, 0, 0, {}};
    bool io = type == kCmdRead || type == kCmdWrite || type == kCmdTrim || type == kCmdWriteZeroes;
    if (io && (off % kBlockSize || len % kBlockSize || off + len > v_.blocks() * kBlockSize || len > kMaxRequest)) {
      it.error = kEINVAL;
    } else if (type == kCmdRead) {
      it.data.resize(len);
      try {
        for (uint32_t i = 0; i < len && !it.error; i += kBlockSize)
          if (v_.read((off + i) / kBlockSize, it.data.data() + i) != ReadStatus::Ok) it.error = kEIO;
      } catch (const std::exception&) {
        it.error = kEIO;
      }
    } else if (type == kCmdWrite || type == kCmdTrim || type == kCmdWriteZeroes) {
      try {
        for (uint32_t i = 0; i < len; i += kBlockSize)
          it.ticket = v_.write_async((off + i) / kBlockSize, type == kCmdWrite ? buf.data() + i : zero);
      } catch (const NoSpace&) {
        it.error = kENOSPC;
      } catch (const std::bad_alloc&) {
        it.error = kENOMEM;
      } catch (const std::exception&) {
        it.error = kEIO;
      }
    } else if (type != kCmdFlush) {  // every replied write is already durable, so FLUSH is a no-op
      it.error = kEINVAL;
    }
    rq.push(std::move(it));
  }
  rq.close();
  replier.join();
}

// ---------------------------------------------------------------- client

NbdClient::NbdClient(const std::string& host, uint16_t port) {
  fd_ = net::connect_tcp(host, port);
  if (fd_ < 0) throw std::runtime_error("nbd: connect failed");
  auto fail = [&](const char* why) {
    ::close(fd_);
    fd_ = -1;
    throw std::runtime_error(std::string("nbd handshake: ") + why);
  };
  uint64_t m1, m2;
  uint16_t hf;
  if (!read_full(fd_, &m1, 8) || !read_full(fd_, &m2, 8) || !read_full(fd_, &hf, 2)) fail("eof");
  if (be64(m1) != kNbdMagic || be64(m2) != kOptMagic) fail("bad magic");
  uint32_t cf = be32(kFixedNewstyle | kNoZeroes);
  std::vector<uint8_t> opt;
  put_be<uint64_t>(opt, kOptMagic), put_be<uint32_t>(opt, kOptGo), put_be<uint32_t>(opt, 6);
  put_be<uint32_t>(opt, 0), put_be<uint16_t>(opt, 0);  // default export, no info requests
  if (!write_full(fd_, &cf, 4) || !write_full(fd_, opt.data(), opt.size())) fail("write");
  for (;;) {
    uint64_t magic = 0;
    uint32_t o = 0, type = 0, len = 0;
    if (!read_full(fd_, &magic, 8) || !read_full(fd_, &o, 4) || !read_full(fd_, &type, 4) ||
        !read_full(fd_, &len, 4))
      fail("eof");
    std::vector<uint8_t> d(be32(len));
    if (!d.empty() && !read_full(fd_, d.data(), d.size())) fail("eof");
    type = be32(type);
    if (type == kRepAck) break;
    if (type == kRepInfo && d.size() >= 12) {
      uint16_t it;
      std::memcpy(&it, d.data(), 2);
      if (be16(it) == kInfoExport) {
        std::memcpy(&size_, d.data() + 2, 8);
        size_ = be64(size_);
      }
    } else if (type & 0x80000000u) {
      fail("server refused NBD_OPT_GO");
    }
  }
}

NbdClient::~NbdClient() {
  if (fd_ >= 0) ::close(fd_);
}

bool NbdClient::send_write(uint64_t handle, uint64_t off, const void* data, uint32_t len) {
  Request q{be32(kReqMagic), 0, be16(kCmdWrite), handle, be64(off), be32(len)};
  return write_full(fd_, &q, sizeof q) && write_full(fd_, data, len);
}

bool NbdClient::send_read(uint64_t handle, uint64_t off, uint32_t len) {
  read_len_[handle] = len;
  Request q{be32(kReqMagic), 0, be16(kCmdRead), handle, be64(off), be32(len)};
  return write_full(fd_, &q, sizeof q);
}

bool NbdClient::send_flush(uint64_t handle) {
  Request q{be32(kReqMagic), 0, be16(kCmdFlush), handle, 0, 0};
  return write_full(fd_, &q, sizeof q);
}

bool NbdClient::recv(Reply& r) {
  SimpleReply s;
  if (!read_full(fd_, &s, sizeof s) || be32(s.magic) != kSimpleReplyMagic) return false;
  r.handle = s.handle;
  r.error = be32(s.error);
  r.data.clear();
  auto it = read_len_.find(r.handle);
  if (it != read_len_.end()) {
    if (!r.error) {
      r.data.resize(it->second);
      if (!read_full(fd_, r.data.data(), r.data.size())) return false;
    }
    read_len_.erase(it);
  }
  return true;
}

void NbdClient::disconnect() {
  if (fd_ < 0) return;
  Request q{be32(kReqMagic), 0, be16(kCmdDisc), 0, 0, 0};
  write_full(fd_, &q, sizeof q);
  ::close(fd_);
  fd_ = -1;
}

}  // namespace blockstore
