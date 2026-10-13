#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <mutex>

#include "blockstore/env.hpp"

#ifdef BLOCKSTORE_HAVE_URING
#include <liburing.h>
#endif

namespace blockstore {
namespace {

[[noreturn]] void fail(const std::string& what) {
  throw IoError(what + ": " + std::strerror(errno));
}

class PosixFile : public File {
 public:
  PosixFile(int fd, std::string path, bool direct, bool prealloc)
      : fd_(fd), path_(std::move(path)), direct_(direct), prealloc_(prealloc) {}
  ~PosixFile() override { ::close(fd_); }

  size_t pread(void* buf, size_t n, uint64_t off) override {
    size_t done = 0;
    while (done < n) {
      ssize_t r = ::pread(fd_, static_cast<char*>(buf) + done, n - done, static_cast<off_t>(off + done));
      if (r < 0 && errno == EINTR) continue;
      if (r < 0) fail("pread " + path_);
      if (r == 0) break;
      done += static_cast<size_t>(r);
    }
    return done;
  }

  void pwrite(const void* buf, size_t n, uint64_t off) override {
    size_t done = 0;
    while (done < n) {
      ssize_t r =
          ::pwrite(fd_, static_cast<const char*>(buf) + done, n - done, static_cast<off_t>(off + done));
      if (r < 0 && errno == EINTR) continue;
      if (r < 0) fail("pwrite " + path_);
      done += static_cast<size_t>(r);
    }
  }

  void sync() override {
#ifdef __APPLE__
    // fsync on macOS does not flush the drive cache; F_FULLFSYNC does.
    if (::fcntl(fd_, F_FULLFSYNC) == 0) return;
    if (::fsync(fd_) != 0) fail("fsync " + path_);
#else
    if (::fdatasync(fd_) != 0) fail("fdatasync " + path_);
#endif
  }

  uint64_t size() override {
    struct stat st;
    if (::fstat(fd_, &st) != 0) fail("fstat " + path_);
    return static_cast<uint64_t>(st.st_size);
  }

  bool direct() const override { return direct_; }

  void preallocate(uint64_t bytes) override {
#ifdef __linux__
    if (prealloc_ && size() == 0) {
      int rc = ::posix_fallocate(fd_, 0, static_cast<off_t>(bytes));
      (void)rc;  // best effort: tmpfs/overlay may refuse
    }
#else
    (void)bytes;
#endif
  }

  int fd() const { return fd_; }

 private:
  int fd_;
  std::string path_;
  bool direct_;
  [[maybe_unused]] bool prealloc_;
};

class PosixEnv : public Env {
 public:
  PosixEnv(std::string dir, PosixOptions opt) : dir_(std::move(dir)), opt_(opt) {
    ::mkdir(dir_.c_str(), 0755);
    struct stat st;
    if (::stat(dir_.c_str(), &st) != 0 || !S_ISDIR(st.st_mode)) fail("mkdir " + dir_);
#ifdef BLOCKSTORE_HAVE_URING
    if (opt_.io != IoMode::Psync) {
      int rc = io_uring_queue_init(kRingEntries, &ring_, 0);
      uring_ = rc == 0;
      if (!uring_) uring_error_ = std::strerror(-rc);
    }
#endif
    if (opt_.io == IoMode::Uring && !uring_)
      throw IoError("io_uring unavailable: " + (uring_error_.empty() ? "not compiled in" : uring_error_));
  }

  ~PosixEnv() override {
#ifdef BLOCKSTORE_HAVE_URING
    if (uring_) io_uring_queue_exit(&ring_);
#endif
  }

  std::shared_ptr<File> open(const std::string& name, bool direct) override {
    std::string path = dir_ + "/" + name;
    int flags = O_RDWR | O_CREAT | O_CLOEXEC;
    bool is_direct = false;
    int fd = -1;
#ifdef O_DIRECT
    if (direct && opt_.direct) {
      fd = ::open(path.c_str(), flags | O_DIRECT, 0644);
      is_direct = fd >= 0;
      if (fd < 0 && errno != EINVAL) fail("open " + path);
      if (fd < 0) direct_refused_ = true;  // filesystem without O_DIRECT (tmpfs, overlay)
    }
#endif
    if (fd < 0) fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) fail("open " + path);
#ifdef __APPLE__
    if (direct && opt_.direct) ::fcntl(fd, F_NOCACHE, 1);
#endif
    if (is_direct) any_direct_ = true;
    return std::make_shared<PosixFile>(fd, path, is_direct, direct && opt_.preallocate);
  }

  bool exists(const std::string& name) override {
    struct stat st;
    return ::stat((dir_ + "/" + name).c_str(), &st) == 0;
  }

  std::vector<std::string> list() override {
    std::vector<std::string> out;
    DIR* d = ::opendir(dir_.c_str());
    if (!d) fail("opendir " + dir_);
    while (dirent* e = ::readdir(d)) {
      std::string n = e->d_name;
      if (n != "." && n != "..") out.push_back(n);
    }
    ::closedir(d);
    return out;
  }

  void remove(const std::string& name) override {
    if (::unlink((dir_ + "/" + name).c_str()) != 0 && errno != ENOENT) fail("unlink " + name);
  }

  void rename(const std::string& from, const std::string& to) override {
    if (::rename((dir_ + "/" + from).c_str(), (dir_ + "/" + to).c_str()) != 0) fail("rename " + from);
  }

  void sync_dir() override {
    int fd = ::open(dir_.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0) fail("open dir " + dir_);
    int rc = ::fsync(fd);
    ::close(fd);
    if (rc != 0) fail("fsync dir " + dir_);
  }

  void write_and_sync(const std::vector<WriteOp>& ops) override {
#ifdef BLOCKSTORE_HAVE_URING
    if (uring_ && ops.size() * 2 <= kRingEntries) return uring_write_and_sync(ops);
#endif
    Env::write_and_sync(ops);
  }

  std::string describe() const override {
    std::string s = uring_ ? "io_uring" : "psync";
#ifdef __APPLE__
    s += opt_.direct ? "+F_NOCACHE" : "";
    s += ", F_FULLFSYNC";
#else
    s += any_direct_ ? "+O_DIRECT" : (direct_refused_ ? " (O_DIRECT refused by fs)" : "");
    s += ", fdatasync";
#endif
    if (!uring_ && !uring_error_.empty()) s += " (io_uring: " + uring_error_ + ")";
    return s;
  }

 private:
#ifdef BLOCKSTORE_HAVE_URING
  // Per file: write -> write -> fdatasync as one linked chain; chains run in parallel.
  void uring_write_and_sync(const std::vector<WriteOp>& ops) {
    std::lock_guard<std::mutex> g(ring_mu_);
    std::vector<File*> files;
    for (auto& op : ops) {
      bool seen = false;
      for (File* f : files) seen |= f == op.file;
      if (!seen) files.push_back(op.file);
    }
    unsigned n = 0;
    for (File* f : files) {
      int fd = static_cast<PosixFile*>(f)->fd();
      for (auto& op : ops) {
        if (op.file != f) continue;
        io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_write(sqe, fd, op.buf, static_cast<unsigned>(op.len), op.off);
        sqe->flags |= IOSQE_IO_LINK;
        io_uring_sqe_set_data64(sqe, op.len);
        n++;
      }
      io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
      io_uring_prep_fsync(sqe, fd, IORING_FSYNC_DATASYNC);
      io_uring_sqe_set_data64(sqe, 0);
      n++;
    }
    int rc = io_uring_submit_and_wait(&ring_, n);
    if (rc < 0) {
      errno = -rc;
      fail("io_uring_submit");
    }
    std::string err;
    for (unsigned i = 0; i < n; i++) {
      io_uring_cqe* cqe = nullptr;
      rc = io_uring_wait_cqe(&ring_, &cqe);
      if (rc < 0) {
        errno = -rc;
        fail("io_uring_wait_cqe");
      }
      uint64_t want = io_uring_cqe_get_data64(cqe);
      if (cqe->res < 0 && err.empty()) err = std::strerror(-cqe->res);
      if (cqe->res >= 0 && static_cast<uint64_t>(cqe->res) != want && err.empty()) err = "short write";
      io_uring_cqe_seen(&ring_, cqe);
    }
    if (!err.empty()) throw IoError("io_uring group commit: " + err);
  }

  static constexpr unsigned kRingEntries = 256;
  io_uring ring_{};
  std::mutex ring_mu_;
#endif
  std::string dir_;
  PosixOptions opt_;
  bool uring_ = false;
  bool any_direct_ = false;
  [[maybe_unused]] bool direct_refused_ = false;
  std::string uring_error_;
};

}  // namespace

std::unique_ptr<Env> posix_env(const std::string& dir, PosixOptions opt) {
  return std::make_unique<PosixEnv>(dir, opt);
}

}  // namespace blockstore
