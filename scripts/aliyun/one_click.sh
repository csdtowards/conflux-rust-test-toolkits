#!/usr/bin/env bash
set -euxo pipefail

if [ $# -lt 2 ]; then
    echo "Parameters required: <key_pair> <instance_count> [<branch_name>] [<repository_url>] [<enable_flamegraph>] "
    exit 1
fi
key_pair="$1"
slave_count=$2
branch="${3:-master}"
repo="${4:-https://github.com/Conflux-Chain/conflux-rust}"
enable_flamegraph=${5:-false}
slave_role=${key_pair}_exp_slave

nodes_per_host=1

run_latency_exp () {
    branch=$1
    exp_config=$2
    tps=$3
    max_block_size_in_bytes=$4

    #1) Create master instance and slave image
    ./create_slave_image.sh $key_pair $branch $repo
    ./ip.sh --public

    #2) Launch slave instances
    master_ip=`cat ips`
    slave_image=`cat slave_image`
    date +"%Y-%m-%d %H:%M:%S"
    ssh -o "StrictHostKeyChecking no" -tt ubuntu@${master_ip} "ulimit -a"
    # ssh ubuntu@${master_ip} "cd ./conflux-rust/tests/extra-test-toolkits/scripts;rm exp.log;rm -rf ~/.ssh/known_hosts;./launch-on-demand.sh $slave_count $key_pair $slave_role $slave_image;"
    ssh -o "StrictHostKeyChecking no" -tt ubuntu@${master_ip} "cd ./conflux-rust/tests/extra-test-toolkits/scripts;rm exp.log;rm -rf ~/.ssh/known_hosts;python3 ./aliyun/launch-on-demand.py --slave $slave_count --key $key_pair --role $slave_role --image $slave_image "
    date +"%Y-%m-%d %H:%M:%S"

    # The images already have the compiled binary setup in `setup_image.sh`,
    # but we can use the following to recompile if we have code updated after image setup.
    #ssh ubuntu@${master_ip} "cd ./conflux-rust/tests/extra-test-toolkits/scripts;export RUSTFLAGS=\"-g\" && cargo build --release ;\
    #parallel-scp -O \"StrictHostKeyChecking no\" -h ips -l ubuntu -p 1000 ../../../target/release/conflux ~ |grep FAILURE|wc -l;"

    #4) Run experiments
    flamegraph_option=""
    if [ $enable_flamegraph = true ]; then
        flamegraph_option="--enable-flamegraph"
    fi
    ssh -tt -o "StrictHostKeyChecking no" -o ServerAliveInterval=60 -o ServerAliveCountMax=120 ubuntu@${master_ip} "cd ./conflux-rust/tests/extra-test-toolkits/scripts;python3 ./exp_latency.py \
    --vms $slave_count \
    --batch-config \"$exp_config\" \
    --storage-memory-gb 16 \
    --bandwidth 100 \
    --tps $tps \
    --send-tx-period-ms 200 \
    $flamegraph_option \
    --nodes-per-host $nodes_per_host \
    --max-block-size-in-bytes $max_block_size_in_bytes \
    --slave-role $slave_role \
    --enable-tx-propagation "

    #5) Terminate slave instances
    # rm -rf tmp_data
    # mkdir tmp_data
    # cd tmp_data
    # ../list-on-demand.sh $slave_role || true
    # ../terminate-on-demand.sh
    # cd ..

    # Download results
    archive_file="exp_stat_latency.tgz"
    log="exp_stat_latency.log"
    scp -o "StrictHostKeyChecking no" ubuntu@${master_ip}:~/conflux-rust/tests/extra-test-toolkits/scripts/${archive_file} .
    tar xfvz $archive_file
    cat $log
    mv $archive_file ${archive_file}.`date +%s`
    mv $log ${log}.`date +%s`

    scp -o "StrictHostKeyChecking no" ubuntu@${master_ip}:~/conflux-rust/tests/extra-test-toolkits/scripts/logs_metrics.tgz .
    rm -fr logs_metrics
    tar xfvz logs_metrics.tgz
    mv logs_metrics.tgz logs_metrics.tgz.`date +%s`

    for file in `ls logs_metrics/*.tgz`
    do
        tar_dir=${file%*.tgz}
        mkdir "$tar_dir"
        tar xzf "$file" -C "$tar_dir"
        rm "$file"
    done
}

# Parameter for one experiment is <block_gen_interval_ms>:<txs_per_block>:<tx_size>:<num_blocks>
# Different experiments in a batch is divided by commas
# Example: "250:1:150000:1000,250:1:150000:1000,250:1:150000:1000,250:1:150000:1000"
exp_config="175:1:300000:2000"

# For experiments with --enable-tx-propagation , <txs_per_block> and <tx_size> will not take effects.
# Block size is limited by `max_block_size_in_bytes`.

tps=1000
max_block_size_in_bytes=450000
echo "start run $branch"
run_latency_exp $branch $exp_config $tps $max_block_size_in_bytes

# Terminate master instance and delete slave images
# Comment this line if the data on the master instances are needed for further analysis
# ./terminate-on-demand.sh
aliyun ecs StopInstances --region us-east-1 --RegionId 'us-east-1' --ForceStop true --InstanceId.1 `cat instances`