#pragma once
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <vector>

#include "blockstore/volume.hpp"

namespace testutil {

using Block = std::vector<uint8_t>;

// Block contents by kind: 0 random (incompressible), 1 compressible, 2 one of a
// small pool (dedup), 3 zeros. The first 16 bytes of kinds 0/1 carry `tag`.
inline Block make_block(uint64_t seed, int kind, uint64_t tag = 0) {
  Block b(blockstore::kBlockSize, 0);
  std::mt19937_64 r(kind == 2 ? 0xd00d + seed % 8 : seed);
  if (kind == 0)
    for (auto& c : b) c = static_cast<uint8_t>(r());
  if (kind == 1) {
    uint8_t pat[24];
    for (auto& c : pat) c = static_cast<uint8_t>('a' + r() % 26);
    for (size_t i = 0; i < b.size(); i++) b[i] = (r() % 16) ? pat[i % 24] : static_cast<uint8_t>(r());
  }
  if (kind == 2)
    for (size_t i = 0; i < b.size(); i++) b[i] = static_cast<uint8_t>(r() % 7);
  if (kind < 2) std::memcpy(b.data(), &tag, 8), std::memcpy(b.data() + 8, &seed, 8);
  return b;
}

inline int pick_kind(std::mt19937_64& rng) {
  int x = static_cast<int>(rng() % 10);
  return x < 4 ? 0 : x < 7 ? 1 : x < 9 ? 2 : 3;
}

inline std::string temp_dir(const char* name) {
  const char* base = std::getenv("TMPDIR");
  std::string tmpl = std::string(base ? base : "/tmp") + "/" + name + "-XXXXXX";
  std::vector<char> buf(tmpl.begin(), tmpl.end());
  buf.push_back(0);
  return mkdtemp(buf.data());
}

inline void rm_rf(const std::string& dir) {
  std::string cmd = "rm -rf '" + dir + "'";
  if (std::system(cmd.c_str()) != 0) {
  }
}

inline uint64_t env_u64(const char* name, uint64_t def) {
  const char* v = std::getenv(name);
  return v ? std::strtoull(v, nullptr, 10) : def;
}

}  // namespace testutil
