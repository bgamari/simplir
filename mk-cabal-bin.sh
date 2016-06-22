#!/bin/bash -e

mkdir -p bin
for i in $(find dist-newstyle/build/ -executable -a -type f -a -! -iname '*.so'); do
    f="`pwd`/$i"
    ln -fs $f bin/$(basename $i)
done

