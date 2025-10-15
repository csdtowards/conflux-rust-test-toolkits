#!/bin/bash

. $(dirname "$0")/copy_logs_lib.sh

set -e

log_dir=$1

expand_logs "$log_dir" ".tgz"
