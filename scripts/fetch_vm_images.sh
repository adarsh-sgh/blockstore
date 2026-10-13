#!/usr/bin/env bash
# Dedup/LZ4 dataset: three official Debian genericcloud amd64 images (two point
# releases of Debian 12 three years apart, plus Debian 13), sha512-verified
# against Debian's SHA512SUMS and converted to raw disks (needs qemu-img).
set -euo pipefail
OUT=${1:-images}
mkdir -p "$OUT"
BASE=https://cloud.debian.org/images/cloud
for img in bookworm/20230612-1409/debian-12-genericcloud-amd64-20230612-1409 \
           bookworm/20261006-2623/debian-12-genericcloud-amd64-20261006-2623 \
           trixie/20261001-2618/debian-13-genericcloud-amd64-20261001-2618; do
  dir=${img%/*}; name=${img##*/}
  if [ ! -f "$OUT/$name.raw" ]; then
    curl -fsSL -o "$OUT/$name.qcow2" "$BASE/$img.qcow2"
    want=$(curl -fsSL "$BASE/$dir/SHA512SUMS" | awk -v f="$name.qcow2" '$2 == f {print $1}')
    got=$( (sha512sum "$OUT/$name.qcow2" 2>/dev/null || shasum -a 512 "$OUT/$name.qcow2") | awk '{print $1}')
    [ "$want" = "$got" ] || { echo "sha512 mismatch for $name" >&2; exit 1; }
    qemu-img convert -O raw "$OUT/$name.qcow2" "$OUT/$name.raw"
    rm "$OUT/$name.qcow2"
  fi
  echo "$OUT/$name.raw"
done
