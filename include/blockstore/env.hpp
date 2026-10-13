#pragma once
#include <cstdint>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace blockstore {

struct IoError : std::runtime_error {
  using std::runtime_error::runtime_error;
};

class File {
 public:
  virtual ~File() = default;
  // Short count only at EOF. Throws IoError.
  virtual size_t pread(void* buf, size_t n, uint64_t off) = 0;
  virtual void pwrite(const void* buf, size_t n, uint64_t off) = 0;
  virtual void sync() = 0;  // everything written so far is durable
  virtual uint64_t size() = 0;
  // O_DIRECT: offsets, lengths and buffers must be 4 KiB aligned.
  virtual bool direct() const { return false; }
  virtual void preallocate(uint64_t /*bytes*/) {}
};

struct WriteOp {
  File* file;
  const void* buf;
  size_t len;
  uint64_t off;
};

// One directory of files. Volume metadata, segments and journals all live in one Env.
class Env {
 public:
  virtual ~Env() = default;
  // Creates the file if missing. `direct` is a hint; check File::direct().
  virtual std::shared_ptr<File> open(const std::string& name, bool direct = false) = 0;
  virtual bool exists(const std::string& name) = 0;
  virtual std::vector<std::string> list() = 0;
  virtual void remove(const std::string& name) = 0;
  virtual void rename(const std::string& from, const std::string& to) = 0;
  virtual void sync_dir() = 0;  // makes creates/renames/removes durable
  // Group commit: issue every write, then make each touched file durable.
  virtual void write_and_sync(const std::vector<WriteOp>& ops);
  virtual std::string describe() const { return "psync"; }
};

enum class IoMode { Auto, Psync, Uring };

struct PosixOptions {
  IoMode io = IoMode::Auto;  // Auto = io_uring when compiled in and the kernel allows it
  bool direct = true;        // O_DIRECT segments on Linux, F_NOCACHE on macOS
  bool preallocate = true;   // fallocate segment files up front (Linux)
  uint64_t prealloc_bytes = 0;
};

std::unique_ptr<Env> posix_env(const std::string& dir, PosixOptions opt = {});

// In-memory filesystem that models power loss: only synced data and synced
// directory entries survive crash(); unsynced writes are dropped, applied,
// torn at a random byte, or replaced by garbage.
class SimEnv : public Env {
 public:
  SimEnv();
  ~SimEnv() override;
  std::shared_ptr<File> open(const std::string& name, bool direct) override;
  bool exists(const std::string& name) override;
  std::vector<std::string> list() override;
  void remove(const std::string& name) override;
  void rename(const std::string& from, const std::string& to) override;
  void sync_dir() override;
  std::string describe() const override { return "sim"; }

  void freeze();  // every later I/O throws, so nothing reaches "disk" after the crash point
  void freeze_after(uint64_t ops);  // freeze when the n-th mutating I/O call is attempted
  void crash(std::mt19937_64& rng);  // apply power-loss model, unfreeze
  // Media corruption: XOR `mask` into byte `off` of a file's content.
  bool flip_bit(const std::string& name, uint64_t off, uint8_t mask);

  struct Impl;

 private:
  std::shared_ptr<Impl> impl_;
};

// Heap buffer aligned for O_DIRECT.
class AlignedBuf {
 public:
  AlignedBuf() = default;
  explicit AlignedBuf(size_t n) { resize(n); }
  AlignedBuf(const AlignedBuf&) = delete;
  AlignedBuf& operator=(const AlignedBuf&) = delete;
  AlignedBuf(AlignedBuf&& o) noexcept { swap(o); }
  AlignedBuf& operator=(AlignedBuf&& o) noexcept {
    swap(o);
    return *this;
  }
  ~AlignedBuf();
  void resize(size_t n);  // keeps contents, zero-fills growth
  void clear() { size_ = 0; }
  uint8_t* data() { return p_; }
  const uint8_t* data() const { return p_; }
  size_t size() const { return size_; }
  void swap(AlignedBuf& o) noexcept {
    std::swap(p_, o.p_);
    std::swap(size_, o.size_);
    std::swap(cap_, o.cap_);
  }

 private:
  uint8_t* p_ = nullptr;
  size_t size_ = 0, cap_ = 0;
};

constexpr uint64_t align_up(uint64_t v, uint64_t a) { return (v + a - 1) / a * a; }
constexpr uint64_t align_down(uint64_t v, uint64_t a) { return v / a * a; }

}  // namespace blockstore
