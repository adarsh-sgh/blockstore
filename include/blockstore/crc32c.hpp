#pragma once
#include <cstddef>
#include <cstdint>

namespace blockstore {

// CRC32C (Castagnoli). Hardware instructions on x86 (SSE4.2) and arm64, table otherwise.
uint32_t crc32c(const void* data, size_t n, uint32_t crc = 0);

}  // namespace blockstore
