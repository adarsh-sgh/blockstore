#include <algorithm>
#include <cstring>
#include <map>
#include <mutex>

#include "blockstore/env.hpp"

namespace blockstore {

namespace {
struct Node {
  std::vector<uint8_t> live, durable;
  struct Write {
    uint64_t off;
    std::vector<uint8_t> data;
  };
  std::vector<Write> pending;  // since last sync, in order
};

void put(std::vector<uint8_t>& v, uint64_t off, const uint8_t* p, size_t n) {
  if (v.size() < off + n) v.resize(off + n);
  std::memcpy(v.data() + off, p, n);
}
}  // namespace

struct SimEnv::Impl {
  std::mutex mu;
  std::map<std::string, std::shared_ptr<Node>> live, durable;
  bool frozen = false;
  uint64_t countdown = 0;  // 0 = disarmed
  void check() {
    if (frozen) throw IoError("sim: crashed");
  }
  void mutate() {  // called before every write, sync and directory change
    check();
    if (countdown && --countdown == 0) {
      frozen = true;
      check();
    }
  }
};

namespace {
class SimFile : public File {
 public:
  SimFile(std::shared_ptr<SimEnv::Impl> env, std::shared_ptr<Node> n) : env_(std::move(env)), n_(std::move(n)) {}
  size_t pread(void* buf, size_t n, uint64_t off) override {
    std::lock_guard<std::mutex> g(env_->mu);
    env_->check();
    if (off >= n_->live.size()) return 0;
    size_t k = std::min<size_t>(n, n_->live.size() - off);
    std::memcpy(buf, n_->live.data() + off, k);
    return k;
  }
  void pwrite(const void* buf, size_t n, uint64_t off) override {
    std::lock_guard<std::mutex> g(env_->mu);
    env_->mutate();
    auto p = static_cast<const uint8_t*>(buf);
    put(n_->live, off, p, n);
    n_->pending.push_back({off, std::vector<uint8_t>(p, p + n)});
  }
  void sync() override {
    std::lock_guard<std::mutex> g(env_->mu);
    env_->mutate();
    n_->durable = n_->live;
    n_->pending.clear();
  }
  uint64_t size() override {
    std::lock_guard<std::mutex> g(env_->mu);
    env_->check();
    return n_->live.size();
  }

 private:
  std::shared_ptr<SimEnv::Impl> env_;
  std::shared_ptr<Node> n_;
};
}  // namespace

SimEnv::SimEnv() : impl_(std::make_shared<Impl>()) {}
SimEnv::~SimEnv() = default;

std::shared_ptr<File> SimEnv::open(const std::string& name, bool) {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->check();
  auto& n = impl_->live[name];
  if (!n) n = std::make_shared<Node>();
  return std::make_shared<SimFile>(impl_, n);
}

bool SimEnv::exists(const std::string& name) {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->check();
  return impl_->live.count(name) > 0;
}

std::vector<std::string> SimEnv::list() {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->check();
  std::vector<std::string> out;
  for (auto& [k, v] : impl_->live) out.push_back(k);
  return out;
}

void SimEnv::remove(const std::string& name) {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->mutate();
  impl_->live.erase(name);
}

void SimEnv::rename(const std::string& from, const std::string& to) {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->mutate();
  auto it = impl_->live.find(from);
  if (it == impl_->live.end()) throw IoError("sim: rename missing " + from);
  impl_->live[to] = it->second;
  impl_->live.erase(from);
}

void SimEnv::sync_dir() {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->mutate();
  impl_->durable = impl_->live;
}

void SimEnv::freeze() {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->frozen = true;
}

void SimEnv::freeze_after(uint64_t ops) {
  std::lock_guard<std::mutex> g(impl_->mu);
  impl_->countdown = ops;
}

void SimEnv::crash(std::mt19937_64& rng) {
  std::lock_guard<std::mutex> g(impl_->mu);
  // Unsynced directory changes are lost; files keep only synced bytes plus a
  // random subset of the unsynced writes, any of which may be torn or garbage.
  impl_->live = impl_->durable;
  for (auto& [name, n] : impl_->live) {
    std::vector<uint8_t> img = n->durable;
    for (auto& w : n->pending) {
      switch (rng() % 4) {
        case 0:  // lost
          break;
        case 1:  // fully applied
          put(img, w.off, w.data.data(), w.data.size());
          break;
        case 2: {  // torn: only a prefix reached the media
          size_t k = rng() % (w.data.size() + 1);
          put(img, w.off, w.data.data(), k);
          break;
        }
        default: {  // garbage over a random sub-range
          size_t a = rng() % (w.data.size() + 1), b = rng() % (w.data.size() + 1);
          if (a > b) std::swap(a, b);
          std::vector<uint8_t> junk(b - a);
          for (auto& c : junk) c = static_cast<uint8_t>(rng());
          put(img, w.off + a, junk.data(), junk.size());
        }
      }
    }
    n->live = n->durable = std::move(img);
    n->pending.clear();
  }
  // Nodes reachable only from the old live namespace are gone.
  impl_->durable = impl_->live;
  impl_->frozen = false;
  impl_->countdown = 0;
}

bool SimEnv::flip_bit(const std::string& name, std::mt19937_64& rng) {
  std::lock_guard<std::mutex> g(impl_->mu);
  auto it = impl_->live.find(name);
  if (it == impl_->live.end() || it->second->live.empty()) return false;
  auto& n = *it->second;
  size_t i = rng() % n.live.size();
  uint8_t bit = static_cast<uint8_t>(1u << (rng() % 8));
  n.live[i] ^= bit;
  if (i < n.durable.size()) n.durable[i] ^= bit;
  return true;
}

}  // namespace blockstore
