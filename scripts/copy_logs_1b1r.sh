#!/bin/bash

. $(dirname "$0")/copy_logs_lib.sh

set -e

log_dir=logs

init_log_dir "$log_dir"

#parallel-ssh -O "StrictHostKeyChecking no" -h ips -p 400 -t 600 'find /tmp/conflux_test_* -name "conflux.log" | xargs tar cvfz log.tgz'
date +"%Y-%m-%d %H:%M:%S"
parallel-ssh -O "StrictHostKeyChecking no" -h ips -p 400 -t 600 "./remote_collect_log_1b1r.sh"
date +"%Y-%m-%d %H:%M:%S"
echo "copy logs"
copy_file_from_slaves_1b1r 1b1r.zst "$log_dir" ".zst"
wait_for_copy "zst"
date +"%Y-%m-%d %H:%M:%S"
# echo "expand logs"
# expand_logs "$log_dir" ".tgz"

