#!/bin/bash

. $(dirname "$0")/copy_logs_lib.sh

# set -e

log_dir=logs

init_log_dir "$log_dir"

#parallel-ssh -O "StrictHostKeyChecking no" -h ips -p 400 -t 600 'find /tmp/conflux_test_* -name "conflux.log" | xargs tar cvfz log.tgz'
date +"%Y-%m-%d %H:%M:%S"
parallel-ssh -O "StrictHostKeyChecking no" -h ips_sample -p 400 -t 600 "./remote_collect_log.sh"
date +"%Y-%m-%d %H:%M:%S"
echo "copy logs"
copy_file_from_slaves log.tgz ips_sample "$log_dir" ".tgz"
wait_for_copy "tgz"
date +"%Y-%m-%d %H:%M:%S"
# echo "expand logs"
# expand_logs "$log_dir" ".tgz"

parallel-ssh -O "StrictHostKeyChecking no" -h ips_metrics -p 400 -t 600 "./remote_collect_metrics.sh"
copy_metrics_from_slaves metrics.log.tgz ips_metrics "$log_dir" ".tgz"
wait_for_copy "tgz"