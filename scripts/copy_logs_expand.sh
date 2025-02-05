#!/bin/bash

. $(dirname "$0")/copy_logs_lib.sh

set -e

log_dir=logs

expand_logs "$log_dir" ".tgz"
