#!/bin/bash -e

mkdir -p bin
for e in $(jq -r '..|."bin-file"?|strings' < dist-newstyle/cache/plan.json); do
    ln -s $e bin/$(basename $e)
done
