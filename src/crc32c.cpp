#include "blockstore/crc32c.hpp"

#include <cstring>

#if defined(__x86_64__) && defined(__SSE4_2__)
#include <nmmintrin.h>
#define BS_CRC_X86 1
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#define BS_CRC_ARM 1
#endif

namespace blockstore {

#if !defined(BS_CRC_X86) && !defined(BS_CRC_ARM)
namespace {
struct Table {
  uint32_t t[256];
  Table() {
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int k = 0; k < 8; k++) c = (c >> 1) ^ (0x82F63B78u & (0u - (c & 1)));
      t[i] = c;
    }
  }
};
const Table kTable;
}  // namespace
#endif

uint32_t crc32c(const void* data, size_t n, uint32_t crc) {
  auto p = static_cast<const uint8_t*>(data);
  crc = ~crc;
#if defined(BS_CRC_X86) || defined(BS_CRC_ARM)
  for (; n >= 8; n -= 8, p += 8) {
    uint64_t v;
    std::memcpy(&v, p, 8);
#if defined(BS_CRC_X86)
    crc = static_cast<uint32_t>(_mm_crc32_u64(crc, v));
#else
    crc = __crc32cd(crc, v);
#endif
  }
  for (; n; n--, p++) {
#if defined(BS_CRC_X86)
    crc = _mm_crc32_u8(crc, *p);
#else
    crc = __crc32cb(crc, *p);
#endif
  }
#else
  for (; n; n--, p++) crc = kTable.t[(crc ^ *p) & 0xff] ^ (crc >> 8);
#endif
  return ~crc;
}

}  // namespace blockstore
