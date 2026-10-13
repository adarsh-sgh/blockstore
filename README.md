# blockstore

A crash-consistent, log-structured block store in C++17. It exposes a thin-provisioned volume
of 4 KiB blocks (in process, or over NBD so `fio`, `qemu` and `nbd-client` can use it). Data
lives in CRC32C-checked append-only segments. Metadata is a write-ahead journal, and one
committer thread group-commits it with `fdatasync`. On top of that: copy-on-write snapshots
over a refcounted block map, inline dedup and LZ4, greedy GC, and synchronous replication to a
standby. A fault-injection suite power-cuts, kill -9s, bit-flips and fails over the store, and
checks that no acknowledged write is ever lost.

```
 NBD clients (fio, qemu)          primary                                  standby
        |                  +----------------------------------+           +-----------+
        v                  | Volume                           |  groups   | Volume    |
   NbdServer --stage-----> |  block map pages --> chunk ids   |---------> | (applies  |
   (pipelined,             |  (refcounted, CoW per snapshot)  |  ack <--- |  logical  |
    in-order replies)      |  chunk table: seg, off, len, crc |           |  ops)     |
                           |  dedup index: xxh3-128 -> chunk  |           +-----------+
                           +---------------+------------------+
                                           | one group in flight
                                           v
               committer: [segment extents (O_DIRECT)] + [journal frame]
                          io_uring: write->fdatasync per file, chains in parallel
                          (pwrite + fdatasync fallback; F_FULLFSYNC on macOS)
```

## Run

```sh
make test          # model, power-loss simulation, bit flips, kill -9, failover (doctest)
make asan          # the same under AddressSanitizer + UBSan
./build/blockstored --dir /var/tmp/vol --blocks 1048576          # NBD on :10809
fio --name=w --ioengine=nbd --uri=nbd://127.0.0.1:10809 --rw=randwrite --bs=4k --iodepth=32 --size=4G

# replication: standby first, then the primary; SIGUSR1 promotes the standby
./build/blockstored --dir /var/tmp/s --standby-port 10810 --nbd-port 10811
./build/blockstored --dir /var/tmp/p --replicate-to 127.0.0.1:10810
kill -USR1 <standby pid>     # -> "promoted; nbd listening on 10811"
```

Linux builds use io_uring when `liburing` is installed. Without it, or when the kernel or a
seccomp profile refuses io_uring, they fall back to `pwrite`. macOS uses `pwrite`, `F_NOCACHE`
and `F_FULLFSYNC`. In Docker, io_uring needs `--security-opt seccomp=unconfined` (see
`Dockerfile`).

## Design

- **Write path.** The writer hashes (XXH3-128), LZ4-compresses and CRC32Cs the block outside the
  lock. Under the lock it either finds the hash in the dedup index (no data written) or appends
  the stored bytes to the active segment's in-memory extent. Then it logs journal records
  (`NewChunk`, `Map`) and applies them to the in-memory state through the same `apply()` that
  replay uses. It gets a ticket (the group sequence number), and `wait(ticket)` returns once
  that group is durable.
- **Group commit.** One committer thread takes everything staged and pads segment extents to
  4 KiB for O_DIRECT. It writes the extents and one CRC32C-framed journal record, `fdatasync`s
  both files, then wakes every writer in the group. With io_uring this is one submission: a
  linked `write -> fdatasync` chain per file, with the chains running in parallel. Only one group is
  in flight, so the next one fills while the current one syncs.
- **Recovery reads metadata only.** Open loads the checkpoint and replays journal frames.
  Because groups commit one at a time, only the *last* frame can name segment data that never
  became durable. Recovery re-reads that frame's payloads, checks their CRCs, and drops the
  frame if any is torn (wiping its header durably, so it cannot resurface once its segment is
  removed as an orphan). A bad frame followed by a valid later frame is media corruption, not a
  torn tail. In that case open refuses with `Corruption` rather than silently truncating
  acknowledged history.
- **Checksums live with the pointer.** A chunk's CRC32C sits in the chunk table, not next to the
  data (as in ZFS block pointers), so misdirected and lost writes are caught as well as bit rot.
  Every read verifies it, and `scrub()` verifies all live chunks.
- **Snapshots are refcounted map pages.** The block map is 512-entry pages. A snapshot copies
  the page-pointer vector and bumps each page's refcount. The first write to a shared page
  copies it and takes a ref on every chunk it names. Chunks are refcounted by the pages that
  name them. Freed chunks leave the dedup index, and their segment's live bytes drop.
- **GC moves chunks, not maps.** The greedy GC picks the sealed segment with the fewest live
  bytes and rewrites its live chunks into the active segment as `Relocate` records. Chunk ids
  are stable, so block maps and snapshots are untouched. A segment file is deleted (or
  recycled) only after the records that emptied it are durable. Writers block when the volume
  is at capacity, and GC has one reserve segment so it can always make progress.
- **Checkpoints** serialize the chunk table, unique map pages and maps. The image is taken in
  the same critical section that hands a group to the committer, and is written only after
  that group is durable. Write order: `tmp -> fsync -> rename -> fsync(dir)`, then a new
  journal file. On load, every refcount is recomputed from the maps; `check_invariants()` does
  the same at runtime.
- **Replication is logical and synchronous.** The committer ships each group's logical ops
  (writes, trims, snapshot ops) to the standby before local I/O starts, and acks clients only
  after both sides are durable. The standby applies the ops through its own write path, so its
  layout, dedup and GC are independent. If the standby is lost, the group fails and the
  primary stops acknowledging writes rather than diverging.
- **Spare segments (opt-in, `spare_segments`).** `fdatasync` after writing into fallocated
  (unwritten) extents costs an ext4 journal commit for the extent conversion: 1.2 ms per 64 KiB
  write+sync in Docker, against about 41 us for a pure overwrite. A background thread can keep
  pre-zeroed segment files ready, and freed segments are recycled into that pool. On the
  runners this cut p50 commit latency by about 35%, but zero-filling cost bandwidth and the
  throughput was mixed (below), so the default is off.

## Results

Linux numbers come from `scripts/bench_linux.sh`, run by the `bench` workflow (two runs on
separate GitHub-hosted runners). The hardware is an **Azure VM**: 4 vCPU AMD EPYC 9V74, 16 GB,
"MSFT NVMe Accelerator" virtual NVMe, ext4 mounted `nobarrier`, kernel 6.17. **This is not bare
NVMe.** With `nobarrier`, `fdatasync` does not flush a device cache, so these are best-case
durability costs on virtual storage. The macOS column is an Apple M5 laptop's internal SSD with
`F_FULLFSYNC`, which really flushes, and it shows what an honest device flush costs.

| What (4 KiB blocks, each write durable before ack) | Azure runner, run 1 | Azure runner, run 2 | Apple M5, macOS |
|---|---|---|---|
| `blockstore_bench randwrite`, 32 writers, io_uring + O_DIRECT | 34.2k IOPS, p50 0.80 ms, p99 2.1 ms | 20.7k IOPS, p50 0.77 ms, p99 7.3 ms | 2.6k IOPS, p50 11.7 ms, p99 23.9 ms (psync, F_FULLFSYNC) |
| same, pwrite + fdatasync | 29.4k IOPS, p99 4.2 ms | 22.5k IOPS, p99 15.3 ms | |
| same, io_uring, 32 pre-zeroed spare segments | 23.8k IOPS, p50 0.53 ms, p99 6.2 ms | 23.4k IOPS, p50 0.52 ms, p99 9.9 ms | |
| `fio --ioengine=nbd --iodepth=32` against `blockstored` | 23.9k IOPS, p99 6.0 ms | 8.8k IOPS, p99 103 ms | |
| baseline: `fio` raw 4 KiB randwrite, QD32, O_DIRECT, no fsync, same fs | 2.8k IOPS | 3.8k IOPS | |
| reopen after 1 GiB of writes: journal replay (15.8 MiB, 524k records) | 92 ms | 118 ms | 124 ms |
| reopen from a checkpoint (same volume) | 58 ms | 79 ms | 89 ms |

- On this disk, random 4 KiB writes through blockstore run 5-12x faster than random writes to
  the raw file, because the log turns them into sequential appends. Group size is about 16 at
  QD32 because one group is in flight while the next one fills.
- io_uring vs pwrite is within the run-to-run noise here: each group is one or two writes plus
  two syncs, so syscall overhead is not the bottleneck. The benefit is the parallel per-file
  chains.
- The two runner instances differed by up to 2.7x, and the NBD row is the most sensitive. Treat
  every row as "this order of magnitude on this class of VM".

**GC write amplification** (`blockstore_bench gc`). Uniform random 4 KiB overwrites, incompressible
unique data, 128 x 4 MiB segments of physical space, measured after one full overwrite pass,
WA = (user bytes + GC-rewritten bytes) / user bytes. These figures come from the algorithm,
not the hardware (same output shape on every machine):

| utilization | 50% | 60% | 70% | 80% | 90% |
|---|---|---|---|---|---|
| write amplification | 1.27 | 1.47 | 1.85 | 2.64 | 4.61 |

**Dedup + LZ4 on VM images** (`scripts/fetch_vm_images.sh` then `blockstore_bench ingest`).
The dataset is three official Debian genericcloud amd64 images, SHA512-verified and converted to
raw: 12 (2023-06-12), 12 (2026-10-06) and 13 (2026-10-01). That is 8 GiB logical, a small VM
fleet of one OS family across releases. Ingested on the M5 at 97 MiB/s.

| config | stored | ratio, non-zero blocks | ratio incl. zero blocks |
|---|---|---|---|
| dedup + LZ4 | 946 MiB | **2.76x** (dedup 1.42x × LZ4 1.94x) | 8.66x |
| LZ4 only | 1353 MiB | 1.93x | 6.05x |
| dedup only | 1837 MiB | 1.42x | 4.46x |

68% of the logical blocks are zeros (unused image space). Zero blocks are never stored: they
unmap the block. So the headline uses the non-zero ratio, and the zero-inclusive one is shown
only for completeness. 198k of 668k non-zero blocks deduplicated, mostly across the two Debian 12 builds.

**Fault injection** (`make test`; counts are from one run and vary with timing):

| Test | What it does | Result |
|---|---|---|
| power-loss simulation | 4 writer threads plus snapshots, GC and checkpoints run on `SimEnv`; it "crashes" after a random number of disk ops, then drops, tears or garbles every unsynced write and loses unsynced directory entries | 200 crashes per CI run (4,000 locally with `BLOCKSTORE_CRASH_ITERS=4000`: 303k acked writes, 159 torn groups dropped), **0 acked writes lost** |
| kill -9 | real `blockstored`, fio-style pipelined NBD client at QD32, SIGKILL at a random time, restart and verify | 12 cycles, about 4-58k acked writes per run, **0 lost** |
| failover | primary replicating to standby, SIGKILL the primary mid-stream, SIGUSR1 promotes the standby, verify there, keep writing | 8 failovers, **0 acked writes lost** |
| standby loss | SIGKILL the standby | every later write gets EIO, **none acked** |
| bit flips | 400-480 random bit flips across segment files | each read either returns correct data or fails its CRC32C (about 300 detected), **0 wrong data returned**; a flip mid-journal or in the checkpoint refuses to open |

The checker is per-LBA: the recovered value must be the last acknowledged version, or a version
issued after it and never acknowledged. Two mutations were checked to fail the suite: skipping
last-frame validation, and skipping the segment `fdatasync`.

## Reproduce

```sh
scripts/bench_linux.sh build /mnt/bench     # Linux: host info, fio baselines, randwrite, NBD+fio, replay, GC
make docker-bench                            # same inside Docker (Docker Desktop disks are too noisy to quote)
scripts/fetch_vm_images.sh images && ./build/blockstore_bench ingest --dir /tmp/dd images/*.raw
BLOCKSTORE_CRASH_ITERS=4000 ./build/blockstore_tests -tc="crash*"
```

## Gaps

- **Throughput is far below a bare-NVMe engine.** There is one group in flight and a global
  metadata mutex, so commits are bounded by sync latency × QD/2. Pipelining groups would need
  recovery to validate the last K frames and allow out-of-order journal tails. That is not built.
- **Numbers are from virtual disks** (Azure `nobarrier`, which skips cache flushes) and one
  laptop SSD. There is no bare-metal NVMe measurement.
- **Replication has no catch-up or fencing.** A standby must start empty with the primary.
  There is no resync after a standby restart, no epochs, and nothing stops a revived old
  primary from serving (split brain is the operator's problem).
- **A bit flip in the last journal frame is indistinguishable from a torn write** and drops
  that group. There is no repair from the replica on CRC failure; reads just return EIO.
- **Dedup trusts XXH3-128 without a byte compare.** That is fine against accidental collisions
  but not adversarial ones. The whole dedup index lives in RAM (about 80 B per unique chunk).
- **GC is greedy and inline in the committer**, which stalls commits while it runs. There is no
  hot/cold separation or cost-benefit policy, and the WA at 90% utilization shows it.
- The NBD server speaks fixed-newstyle with simple replies only: no structured replies, no
  multi-connection, and requests must be 4 KiB aligned.
