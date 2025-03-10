#!/bin/bash


find /tmp/conflux_test_* -name conflux.log | xargs grep -i "\[1b1r\]" > 1b1r.log

# tar cvfz log.tgz *.log
zstd -12 1b1r.log -o 1b1r.zst

rm *.log
