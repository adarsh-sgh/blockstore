JOBS ?= 8

.PHONY: build test asan bench docker-bench clean

build:
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
	cmake --build build -j $(JOBS)

test: build
	./build/blockstore_tests

asan:
	cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DBLOCKSTORE_ASAN=ON
	cmake --build build-asan -j $(JOBS)
	./build-asan/blockstore_tests

# Linux benchmarks (io_uring + O_DIRECT) in Docker; results land in bench/results/.
docker-bench:
	./scripts/docker_bench.sh

clean:
	rm -rf build build-asan
