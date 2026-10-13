# Linux build/bench image: io_uring + O_DIRECT path, fio with the NBD engine.
FROM ubuntu:24.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      build-essential cmake liburing-dev fio python3 ca-certificates curl xz-utils qemu-utils \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
