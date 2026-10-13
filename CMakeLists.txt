cmake_minimum_required(VERSION 3.16)
project(blockstore C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

option(BLOCKSTORE_ASAN "Build with AddressSanitizer + UBSan" OFF)
option(BLOCKSTORE_URING "Use io_uring when liburing is available (Linux)" ON)

find_package(Threads REQUIRED)

# Vendored: LZ4 1.10.0, xxHash 0.8.2 (header only), doctest.
add_library(lz4 STATIC third_party/lz4.c)
target_include_directories(lz4 SYSTEM PUBLIC third_party)

add_library(blockstore STATIC
  src/crc32c.cpp
  src/env.cpp
  src/posix_env.cpp
  src/sim_env.cpp
  src/volume.cpp
  src/net.cpp
  src/nbd.cpp
  src/replication.cpp)
target_include_directories(blockstore PUBLIC include)
target_compile_options(blockstore PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(blockstore PUBLIC Threads::Threads lz4)

# Hardware CRC32C.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
  target_compile_options(blockstore PRIVATE -msse4.2)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64" AND NOT APPLE)
  target_compile_options(blockstore PRIVATE -march=armv8-a+crc)
endif()

if(BLOCKSTORE_URING AND CMAKE_SYSTEM_NAME STREQUAL "Linux")
  find_path(URING_INCLUDE liburing.h)
  find_library(URING_LIB uring)
  if(URING_INCLUDE AND URING_LIB)
    message(STATUS "io_uring: ${URING_LIB}")
    target_compile_definitions(blockstore PUBLIC BLOCKSTORE_HAVE_URING)
    target_include_directories(blockstore PRIVATE ${URING_INCLUDE})
    target_link_libraries(blockstore PUBLIC ${URING_LIB})
  else()
    message(STATUS "io_uring: liburing not found, pwrite fallback only")
  endif()
endif()

if(BLOCKSTORE_ASAN)
  foreach(t blockstore lz4)
    target_compile_options(${t} PUBLIC -fsanitize=address,undefined -fno-sanitize-recover=undefined -fno-omit-frame-pointer)
    target_link_options(${t} PUBLIC -fsanitize=address,undefined)
  endforeach()
endif()

add_executable(blockstored src/main_server.cpp)
add_executable(blockstore_bench src/main_bench.cpp)
foreach(t blockstored blockstore_bench)
  target_link_libraries(${t} PRIVATE blockstore)
  target_compile_options(${t} PRIVATE -Wall -Wextra -Wpedantic)
endforeach()

enable_testing()
add_executable(blockstore_tests
  tests/test_main.cpp
  tests/test_volume.cpp
  tests/test_crash.cpp
  tests/test_faults.cpp)
target_include_directories(blockstore_tests PRIVATE third_party)
target_link_libraries(blockstore_tests PRIVATE blockstore)
target_compile_definitions(blockstore_tests PRIVATE BLOCKSTORED_PATH="$<TARGET_FILE:blockstored>")
add_dependencies(blockstore_tests blockstored)
add_test(NAME blockstore_tests COMMAND blockstore_tests)
