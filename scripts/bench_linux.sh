#!/usr/bin/env bash
# Every Linux number in the README. Usage: scripts/bench_linux.sh BUILD_DIR DATA_DIR
# DATA_DIR must be on a real filesystem (ext4/xfs), not overlay or tmpfs, for O_DIRECT.
set -euo pipefail
B=${1:-build}; D=${2:-/tmp/blockstore-bench}
SECONDS_RUN=${SECONDS_RUN:-15}
mkdir -p "$D"
fresh() { rm -rf "$D/vol"; sync; sleep "${SETTLE:-5}"; }

echo "== host"
uname -sr; nproc; (lscpu | grep -E "Model name|^CPU\(s\)") || true
free -g | head -2; df -hT "$D" | tail -1; findmnt -T "$D" -no SOURCE,FSTYPE,OPTIONS || true
lsblk -d -o NAME,SIZE,ROTA,MODEL 2>/dev/null || true

echo "== fio baselines on the same filesystem (no blockstore)"
fio --name=raw-randwrite-qd32 --filename="$D/fio.dat" --size=4G --ioengine=io_uring --direct=1 \
    --rw=randwrite --bs=4k --iodepth=32 --runtime="$SECONDS_RUN" --time_based --group_reporting \
    | grep -E "IOPS|99.00th" | head -2
fio --name=raw-write-fdatasync-qd1 --filename="$D/fio.dat" --size=4G --ioengine=psync --direct=1 \
    --rw=randwrite --bs=4k --fdatasync=1 --runtime=10 --time_based | grep -E "IOPS|sync.*avg"
rm -f "$D/fio.dat"

echo "== blockstore randwrite, in process (4 KiB, 32 writers, each write durable)"
for io in uring psync; do
  for prep in 0 32; do
    fresh
    "$B/blockstore_bench" randwrite --dir "$D/vol" --io $io --qd 32 --seconds "$SECONDS_RUN" \
        --prepare-segments $prep --settle "${SETTLE:-5}"
  done
done

echo "== blockstore over NBD, driven by fio (ioengine=nbd, iodepth=32)"
fresh
"$B/blockstored" --dir "$D/vol" --blocks 1048576 --host 127.0.0.1 & SRV=$!
sleep 1
fio --name=nbd-randwrite-qd32 --ioengine=nbd --uri=nbd://127.0.0.1:10809 --rw=randwrite --bs=4k \
    --iodepth=32 --size=4G --runtime="$SECONDS_RUN" --time_based | grep -E "IOPS|clat \(|99.00th"
kill $SRV; wait $SRV || true

echo "== journal replay"
fresh
"$B/blockstore_bench" replay --dir "$D/vol" --mib 1024

echo "== GC write amplification"
rm -rf "$D/gc"
"$B/blockstore_bench" gc --dir "$D/gc" --utils 0.5,0.6,0.7,0.8,0.9 --segment-mb 4 --capacity 128 --passes 3
rm -rf "$D/gc" "$D/vol"
