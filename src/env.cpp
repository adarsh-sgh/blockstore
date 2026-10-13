#include <cstdlib>
#include <cstring>
#include <set>

#include "blockstore/env.hpp"

namespace blockstore {

void Env::write_and_sync(const std::vector<WriteOp>& ops) {
  std::vector<File*> files;
  for (auto& op : ops) {
    op.file->pwrite(op.buf, op.len, op.off);
    bool seen = false;
    for (File* f : files) seen |= f == op.file;
    if (!seen) files.push_back(op.file);
  }
  for (File* f : files) f->sync();
}

AlignedBuf::~AlignedBuf() { std::free(p_); }

void AlignedBuf::resize(size_t n) {
  if (n > cap_) {
    size_t cap = std::max<size_t>(align_up(n, 4096), cap_ * 2);
    void* p = nullptr;
    if (posix_memalign(&p, 4096, cap) != 0) throw std::bad_alloc();
    if (size_) std::memcpy(p, p_, size_);
    std::free(p_);
    p_ = static_cast<uint8_t*>(p);
    cap_ = cap;
  }
  if (n > size_) std::memset(p_ + size_, 0, n - size_);
  size_ = n;
}

}  // namespace blockstore
