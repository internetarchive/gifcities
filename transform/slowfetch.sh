#!/bin/env bash

# the purpose of this script is to read about gifs in jsonl on STDIN and hit
# live wayback for each one. This script is to fill in the gaps we hit when
# using the ADS mirror of wayback which, it turns out, is missing thousands of
# files do to issues in the snapshotting process. It's understood that this is
# the worst way to fetch things in bulk but the number of gifs we need is
# "small" enough (4500) that a serial script made more sense than trying to fix
# the ADS snapshots.

mkdir -p missing
seen=`ls missing`

while read x; do
  cksum=`echo $x | jq -r .checksum`

  if [ -z "$cksum" ]; then
    echo "empty cksum: $x"
    exit 3
  fi

  if echo $seen | grep -q $cksum; then
    echo "SKIP $cksum"
    continue
  fi

  ts=`echo $x | jq -r .uses[0].timestamp`
  url=`echo $x | jq -r .uses[0].url`

  if [ -z "$ts" ]; then
    echo "empty timestamp: $x"
    exit 1
  fi

  if [ -z "$url" ]; then
    echo "empty url: $x"
    exit 2
  fi

  wget https://web.archive.org/web/$ts/$url -O missing/$cksum
  sleep 1
done
