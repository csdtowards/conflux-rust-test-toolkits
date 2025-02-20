#!/usr/bin/env python3
import sys, os, json, re
sys.path.insert(1, os.path.dirname(sys.path[0]))

from argparse import ArgumentParser, SUPPRESS
from collections import Counter
import eth_utils
import rlp
import tarfile
from concurrent.futures import ThreadPoolExecutor

import conflux.config
from conflux.rpc import RpcClient
from conflux.utils import encode_hex, bytes_to_int, priv_to_addr, parse_as_int, pub_to_addr
from test_framework.test_framework import ConfluxTestFramework, OptionHelper
from test_framework.util import *
import time
from scripts.stat_latency_map_reduce import Statistics
import platform

CONFIRMATION_THRESHOLD = 0.1**6 * 2**256

def execute(cmd, retry, cmd_description):
    while retry > 0:
        ret = os.system(cmd)

        if platform.system().lower() == "linux":
            ret = os.waitstatus_to_exitcode(ret)

        if ret == 0:
            return 0

        print("Failed to {}, return code = {}, retry = {} ...".format(cmd_description, ret, retry))
        # assert retry > 0
        retry -= 1
        if retry == 0:
            return ret
        time.sleep(1)

def pssh(ips_file:str, remote_cmd:str, retry=3, cmd_description="", output="> /dev/null 2>&1"):
    cmd = f'parallel-ssh -O "StrictHostKeyChecking no" -h {ips_file} -p 200 "{remote_cmd}" {output}'
    print(cmd)
    return execute(cmd, retry, cmd_description)

def pssh_ips(ips:str, remote_cmd:str, retry=3, cmd_description="", output="> /dev/null 2>&1"):
    cmd = f'parallel-ssh -O "StrictHostKeyChecking no" -H "{ips}" -p 200 "{remote_cmd}" {output}'
    print(f"{cmd}")
    return execute(cmd, retry, cmd_description)
    
def pscp(ips_file:str, local:str, remote:str, retry=3, cmd_description="", output="> /dev/null 2>&1"):
    cmd = f'parallel-scp -O "StrictHostKeyChecking no" -h {ips_file} -p 200 {local} {remote} {output}'
    return execute(cmd, retry, cmd_description)

def kill_remote_conflux(ips_file:str):
    ret = pssh(ips_file, "killall conflux || echo already killed", 1, "kill remote conflux", "> killConflux 2>&1")
    if ret > 0:
        failure_pattern = r"\[FAILURE\] (\d+\.\d+\.\d+\.\d+)"
        with open("killConflux", "r") as f:
            content = f.read()
            failure_ips = set(re.findall(failure_pattern, content))
            print(f"Failure IPs: {failure_ips}")
            for ip in failure_ips:
                cmd = f'ssh -o "StrictHostKeyChecking no" {ip} "killall conflux || echo already killed" > /dev/null 2>&1'
                if execute(cmd, 5, "kill remote conflux") > 0:
                    print(f"Failed to kill conflux on {ip}")

"""
Setup and run conflux nodes on multiple vms with a few nodes on each vm.
"""
class RemoteSimulate(ConfluxTestFramework):
    def set_test_params(self):
        self.rpc_timewait = 600
        # Have to have a num_nodes due to assert in base class.
        self.num_nodes = None

    SIMULATE_OPTIONS = dict(
        # Bandwidth in Mbit/s
        bandwidth = 20,
        connect_peers = 3,
        enable_flamegraph = False,
        enable_tx_propagation = False,
        ips_file = "ips",
        ips_file_sample = "ips_sample",
        generation_period_ms = 500,
        nodes_per_host = 3,
        num_blocks = 1000,
        report_progress_blocks = 10,
        storage_memory_gb = 2,
        tps = 1000,
        txs_per_block = 1,
        generate_tx_data_len = 0,
    )

    PASS_TO_CONFLUX_OPTIONS = dict(
        egress_min_throttle = 512,
        egress_max_throttle = 1024,
        egress_queue_capacity = 2048,
        genesis_secrets = "/home/ubuntu/genesis_secrets.txt",
        send_tx_period_ms = 1300,
        txgen_account_count = 100,
        txgen_batch_size = 10,
        tx_pool_size = conflux.config.default_conflux_conf["tx_pool_size"],
        max_block_size_in_bytes = conflux.config.default_config["MAX_BLOCK_SIZE_IN_BYTES"],
        execution_prefetch_threads = 4,
        # pos
        hydra_transition_number = 4294967295,
        hydra_transition_height = 4294967295,
        pos_reference_enable_height = 4294967295,
        cip43_init_end_number = 4294967295,
        sigma_fix_transition_number = 4294967295,
        public_rpc_apis="cfx,debug,test,pubsub,trace",

        cip1559_transition_height = 4294967295,
        target_block_gas_limit = 6_000_000_000
    )

    def add_options(self, parser:ArgumentParser):
        OptionHelper.add_options(parser, RemoteSimulate.SIMULATE_OPTIONS)
        OptionHelper.add_options(parser, RemoteSimulate.PASS_TO_CONFLUX_OPTIONS)

    def after_options_parsed(self):
        ConfluxTestFramework.after_options_parsed(self)

        # num_nodes is set to nodes_per_host because setup_chain() generates configs
        # for each node on the same host with different port number.
        self.num_nodes = self.options.nodes_per_host
        self.enable_tx_propagation = self.options.enable_tx_propagation
        self.ips = []
        with open(self.options.ips_file, 'r') as ip_file:
            for line in ip_file.readlines():
                line = line[:-1]
                self.ips.append(line)
        
        self.new_ips = {}
        if os.path.isfile("instances.json"):
            with open("instances.json", "r") as f:
                data = json.load(f)
                for k, v in data.items():
                    self.new_ips[int(k)] = v
        self.num_nodes = max(self.new_ips.keys())
        
        self.conf_parameters = OptionHelper.conflux_options_to_config(
            vars(self.options), RemoteSimulate.PASS_TO_CONFLUX_OPTIONS)

        # Default Conflux memory consumption
        target_memory = 16
        # Overwrite with scaled configs so that Conflux consumes storage_memory_gb rather than target_memory.
        for k in ["db_cache_size", "ledger_cache_size",
            "storage_delta_mpts_cache_size", "storage_delta_mpts_cache_start_size",
            "storage_delta_mpts_slab_idle_size"]:
            self.conf_parameters[k] = str(
                conflux.config.production_conf[k] // target_memory * self.options.storage_memory_gb)
        self.conf_parameters["tx_pool_size"] = \
            self.options.tx_pool_size // target_memory * self.options.storage_memory_gb

        # Do not keep track of tx index to save CPU/Disk costs because they are not used in the experiments
        self.conf_parameters["persist_tx_index"] = "false"

        if self.enable_tx_propagation:
            self.conf_parameters["generate_tx"] = "true"
            self.conf_parameters["generate_tx_period_us"] = str(1000000 * len(self.ips) // self.options.tps)
        else:
            self.conf_parameters["send_tx_period_ms"] = "31536000000" # one year to disable txs propagation
            del self.conf_parameters["genesis_secrets"]
        # FIXME: Double check if disabling this improves performance.
        self.conf_parameters["enable_optimistic_execution"] = "false"

    def stop_nodes(self):
        # kill_remote_conflux(self.options.ips_file)
        pass

    def setup_remote_conflux(self):
        # tar the config file for all nodes
        zipped_conf_file = os.path.join(self.options.tmpdir, "conflux_conf.tgz")
        with tarfile.open(zipped_conf_file, "w:gz") as tar_file:
            tar_file.add(self.options.tmpdir, arcname=os.path.basename(self.options.tmpdir))

        self.log.info("copy conflux configuration files to remote nodes ...")
        pscp(self.options.ips_file, zipped_conf_file, "~", 3, "copy conflux configuration files to remote nodes")
        os.remove(zipped_conf_file)

        # setup on remote nodes and start conflux
        self.log.info("setup conflux runtime environment and start conflux on remote nodes ...")
        cmd_kill_conflux = "killall -9 conflux || echo already killed"
        cmd_cleanup = "rm -rf /tmp/conflux_test_*"
        cmd_setup = "tar zxf conflux_conf.tgz -C /tmp"
        cmd_startup = "./remote_start_conflux.sh {} {} {} {} {}&> start_conflux.out".format(
            self.options.tmpdir, p2p_port(0), self.options.nodes_per_host,
            self.options.bandwidth, str(self.options.enable_flamegraph).lower()
        )
        cmd = "{}; {} && {} && {}".format(cmd_kill_conflux, cmd_cleanup, cmd_setup, cmd_startup)
        pssh(self.options.ips_file, cmd, 3, "setup and run conflux on remote nodes", "> runconflux 2>&1")

    def setup_remote_conflux_with_ips(self):
        # tar the config file for all nodes
        zipped_conf_file = os.path.join(self.options.tmpdir, "conflux_conf.tgz")
        with tarfile.open(zipped_conf_file, "w:gz") as tar_file:
            tar_file.add(self.options.tmpdir, arcname=os.path.basename(self.options.tmpdir))

        self.log.info("copy conflux configuration files to remote nodes ...")
        ret = pscp(self.options.ips_file, zipped_conf_file, "~", 1, "copy conflux configuration files to remote nodes", "> copyconfluxcfg 2>&1")
        if ret > 0:
            failure_pattern = r"\[FAILURE\] (\d+\.\d+\.\d+\.\d+)"
            with open("copyconfluxcfg", "r") as f:
                content = f.read()
                failure_ips = set(re.findall(failure_pattern, content))
                print(f"Failure IPs: {failure_ips}")
                for ip in failure_ips:
                    cmd = f'scp -o "StrictHostKeyChecking no" {zipped_conf_file} {ip}:~ > /dev/null 2>&1'
                    if execute(cmd, 5, "copy conflux configuration files to remote nodes") > 0:
                        print(f"Failed to copy conflux configuration files to remote nodes on {ip}")
                    
        os.remove(zipped_conf_file)

        # setup on remote nodes and start conflux
        self.log.info("setup conflux runtime environment and start conflux on remote nodes ...")
        cmd_kill_conflux = "killall -9 conflux || echo already killed"
        cmd_cleanup = "rm -rf /tmp/conflux_test_*"
        cmd_setup = "tar zxf conflux_conf.tgz -C /tmp"


        for nodes_per_host, ips in self.new_ips.items():
            print(f"ip len {len(ips)}, {ips[0]}")
            cmd_startup = "./remote_start_conflux.sh {} {} {} {} {}&> start_conflux.out".format(
                self.options.tmpdir, p2p_port(0), nodes_per_host,
                self.options.bandwidth, str(self.options.enable_flamegraph).lower()
            )
            cmd = "{}; {} && {} && {}".format(cmd_kill_conflux, cmd_cleanup, cmd_setup, cmd_startup)
            with open("ips_tmp", "w") as file:
                for item in ips:
                    file.write(f"{item}\n")

            ret = pssh("ips_tmp", cmd, 1, "setup and run conflux on remote nodes", "> remotestartconflux 2>&1")
            if ret > 0:
                failure_pattern = r"\[FAILURE\] (\d+\.\d+\.\d+\.\d+)"
                with open("remotestartconflux", "r") as f:
                    content = f.read()
                    failure_ips = set(re.findall(failure_pattern, content))
                    print(f"Failure IPs: {failure_ips}")
                    for ip in failure_ips:
                        new_cmd = f'ssh -o "StrictHostKeyChecking no" {ip} "{cmd}" > /dev/null 2>&1'
                        if execute(new_cmd, 5, "setup and run conflux on remote nodes") > 0:
                            print(f"Failed to setup and run conflux on remote nodes on {ip}")
            
            # add remote nodes and start all
            for ip in ips:
                self.add_remote_nodes(nodes_per_host, user="ubuntu", ip=ip, no_pssh=False)

        
    def setup_network(self):
        if self.new_ips is None or len(self.new_ips) == 0:
            self.setup_remote_conflux()

            # add remote nodes and start all
            for ip in self.ips:
                self.add_remote_nodes(self.options.nodes_per_host, user="ubuntu", ip=ip, no_pssh=False)
        else:
            self.setup_remote_conflux_with_ips()
            
        for i in range(len(self.nodes)):
            self.log.info("Node[{}]: ip={}, p2p_port={}, rpc_port={}".format(
                i, self.nodes[i].ip, self.nodes[i].port, self.nodes[i].rpcport))
        self.log.info("Starting remote nodes ...")
        self.start_nodes()
        self.log.info("All nodes started, waiting to be connected")

        connect_sample_nodes(self.nodes, self.log, sample=self.options.connect_peers, timeout=120, assert_failure=False)

        self.wait_until_nodes_synced()


    def start_nodes(self, extra_args=None, *args, **kwargs):
        """Start multiple bitcoinds"""

        try:
            for i, node in enumerate(self.nodes):
                try:
                    node.start(extra_args, *args, **kwargs)
                except Exception as e:
                    self.log.info("start node failed")
                    self.log.error(str(e))

            for i, node in enumerate(self.nodes):
                try:
                    self.log.info("node {} {}".format(i, node.ip))
                    node.wait_for_rpc_connection()
                    node.wait_for_nodeid()
                    node.wait_for_recovery(["NormalSyncPhase"], 10)
                except Exception as e:
                    self.log.error("Failed to start node {} {}".format(i, node.ip))
                    self.log.error(str(e))
        except:
            # If one node failed to start, stop the others
            # self.stop_nodes()
            # raise
            self.log.info("skip failed node")

                
    def init_txgen(self):
        if self.enable_tx_propagation:
            #setup usable accounts
            start_time = time.time()
            current_index=0
            for i in range(len(self.nodes)):
                client = RpcClient(self.nodes[i])
                client.send_usable_genesis_accounts(current_index)
                # Each node use independent set of txgen_account_count genesis accounts.
                current_index+=self.options.txgen_account_count
            self.log.info("Time spend (s) on setting up genesis accounts: {}".format(time.time()-start_time))

    def generate_blocks_async(self):
        num_nodes = len(self.nodes)

        max_retry = 200
        # generate blocks
        threads = {}
        rpc_times = []
        for i in range(1, self.options.num_blocks + 1):
            wait_sec = random.expovariate(1000 / self.options.generation_period_ms)
            start = time.time()

            # find an idle node to generate block
            p = random.randint(0, num_nodes - 1)
            retry = 0
            while retry < max_retry:
                pre_thread = threads.get(p)
                if pre_thread is not None and pre_thread.is_alive():
                    p = random.randint(0, num_nodes - 1)
                    retry += 1
                    time.sleep(0.05)
                else:
                    break

            if retry >= max_retry:
                self.log.warn("too many nodes are busy to generate block, stop to analyze logs.")
                break

            if self.enable_tx_propagation:
                # Generate a block with the transactions in the node's local tx pool
                thread = SimpleGenerateThread(self.nodes, p, self.options.max_block_size_in_bytes, self.log, rpc_times,
                                              self.confirm_info)
            else:
                # Generate a fixed-size block with fake tx
                thread = GenerateThread(self.nodes, p, self.options.txs_per_block, self.options.generate_tx_data_len,
                                        self.options.max_block_size_in_bytes, self.log, rpc_times, self.confirm_info)
            thread.start()
            threads[p] = thread

            if i % self.options.report_progress_blocks == 0:
                self.log.info("[PROGRESS] %d blocks generated async", i)

            self.progress = i

            elapsed = time.time() - start
            if elapsed < wait_sec:
                self.log.debug("%d generating block %.2f", p, elapsed)
                time.sleep(wait_sec - elapsed)
            elif elapsed > 0.01:
                self.log.warning("%d generating block slowly %.2f", p, elapsed)
        self.log.info("generateoneblock RPC latency: {}".format(Statistics(rpc_times, 3).__dict__))
        self.log.info(f"average confirmation latency: {self.confirm_info.get_average_latency()}")

    def gather_confirmation_latency_async(self):
        executor = ThreadPoolExecutor()
        query_count = 80

        def get_risk(block):
            p = random.randint(0, len(self.nodes) - 1)
            try:
                risk = self.nodes[p].cfx_getConfirmationRiskByHash(block)
                self.log.debug(f"risk: {block} {risk}")
                return (block, risk)
            except Exception as e:
                self.log.info("get risk failed {}".format(str(e)))
                return (None, None)

        while not self.stopped:
            futures = []
            for block in self.confirm_info.get_unconfirmed_blocks()[:query_count]:
                futures.append(executor.submit(get_risk, block))
            for f in futures:
                block, risk = f.result()
                if risk is not None and int(risk, 16) <= CONFIRMATION_THRESHOLD:
                    self.confirm_info.confirm_block(block)
            self.log.info(self.confirm_info.progress())
            time.sleep(0.5)

    def run_test(self):
        # setup monitor to report the current block count periodically
        cur_block_count = self.nodes[0].test_getBlockCount()
        # The monitor will check the block_count of nodes[0]
        self.progress = 0
        self.stopped = False
        self.confirm_info = BlockConfirmationInfo()
        monitor_thread = threading.Thread(target=self.monitor, args=(cur_block_count, 1600), daemon=True)
        monitor_thread.start()
        threading.Thread(target=self.gather_confirmation_latency_async, daemon=True).start()
        # When enable_tx_propagation is set, let conflux nodes generate tx automatically.
        self.init_txgen()

        # We instruct nodes to generate blocks.
        self.generate_blocks_async()

        monitor_thread.join()
        self.stopped = True

        i = 0
        while i < len(self.nodes):
            try:
                self.log.info("Goodput: {}".format(self.nodes[i].test_getGoodPut()))
                break
            except Exception as e:
                i += 1
                self.log.info("get goodput failed {}".format(str(e)))

        self.wait_until_nodes_synced()

        ghost_confirmation_time = []
        node0 = RpcClient(self.nodes[0])
        self.log.info("Best block: {}, height: {}".format(node0.best_block_hash(), node0.epoch_number()))
        for i in range(1, node0.epoch_number()+1):
            pivot_block = node0.block_by_epoch(node0.EPOCH_NUM(i))["hash"]
            if pivot_block in self.confirm_info.block_confirmation_time:
                ghost_confirmation_time.append(self.confirm_info.block_confirmation_time[pivot_block])
        if len(ghost_confirmation_time) != 0:
            self.log.info("GHOST average confirmation time: {} confirmed number: {}".format(
                sum(ghost_confirmation_time)/len(ghost_confirmation_time),
                len(ghost_confirmation_time)
            ))


    def wait_until_nodes_synced(self):
        """
        Wait for all nodes to reach same block count and best block
        """
        self.log.info("wait for all nodes to sync blocks ...")

        executor = ThreadPoolExecutor()

        start = time.time()
        # Wait for at most 120 seconds
        while time.time() - start <= 120:
            block_counts = []
            best_blocks = []
            block_count_futures = []
            best_block_futures = []

            for i in range(len(self.nodes)):
                n = self.nodes[i]
                block_count_futures.append(executor.submit(n.test_getBlockCount))
                best_block_futures.append(executor.submit(n.best_block_hash))

            for f in block_count_futures:
                # assert f.exception() is None, "failed to get block count: {}".format(f.exception())
                if f.exception():
                    self.log.info("failed to get block count: {}".format(f.exception()))
                else:
                    block_counts.append(f.result())
            max_count = max(block_counts)
            for i in range(len(block_counts)):
                if block_counts[i] < max_count - 50:
                    self.log.info("Slow: {}: {}".format(i, block_counts[i]))

            for f in best_block_futures:
                # assert f.exception() is None, "failed to get best block: {}".format(f.exception())
                if f.exception():
                    self.log.info("failed to get best block: {}".format(f.exception()))
                else:
                    best_blocks.append(f.result())

            self.log.info("blocks: {}".format(Counter(block_counts)))

            if block_counts.count(block_counts[0]) == len(self.nodes) and best_blocks.count(best_blocks[0]) == len(self.nodes):
                break

            time.sleep(5)
        executor.shutdown()

    def monitor(self, cur_block_count:int, retry_max:int):
        pre_block_count = 0

        retry = 0
        while pre_block_count < self.options.num_blocks + cur_block_count:
            time.sleep(self.options.generation_period_ms / 1000 / 2)

            # block count
            block_count = self.nodes[0].test_getBlockCount()
            if block_count != pre_block_count:
                gap = self.progress + cur_block_count - block_count
                self.log.info("current blocks: %d (gaps: %d)", block_count, gap)
                pre_block_count = block_count
                retry = 0
            else:
                retry += 1
                if retry >= retry_max:
                    self.log.error("No block generated after %d average block generation intervals", retry_max / 2)
                    break

        self.log.info("monitor completed.")


class BlockConfirmationInfo:
    def __init__(self):
        self.block_start_time = {}
        self.block_confirmation_time = {}
        self.unconfirmed_block = set()
        self._lock = threading.Lock()

    def add_block(self, h):
        self._lock.acquire()
        self.block_start_time[h] = time.time()
        self.unconfirmed_block.add(h)
        self._lock.release()

    def confirm_block(self, h):
        self._lock.acquire()
        self.block_confirmation_time[h] = time.time() - self.block_start_time[h]
        self.unconfirmed_block.remove(h)
        self._lock.release()

    def get_unconfirmed_blocks(self):
        self._lock.acquire()
        sorted_blocks = sorted(self.unconfirmed_block, key=lambda h: self.block_start_time[h])
        self._lock.release()
        return sorted_blocks

    def get_average_latency(self):
        self._lock.acquire()
        confirmation_time = self.block_confirmation_time.values()
        self._lock.release()
        if len(confirmation_time) == 0:
            return 0
        return sum(confirmation_time) / len(confirmation_time)

    def progress(self):
        self._lock.acquire()
        s = f"generated: {len(self.block_start_time)}, confirmed: {len(self.block_confirmation_time)}"
        self._lock.release()
        return s

class GenerateThread(threading.Thread):
    def __init__(self, nodes, i, tx_n, tx_data_len, max_block_size, log, rpc_times:list, confirm_info: BlockConfirmationInfo):
        threading.Thread.__init__(self, daemon=True)
        self.nodes = nodes
        self.i = i
        self.tx_n = tx_n
        self.tx_data_len = tx_data_len
        self.max_block_size = max_block_size
        self.log = log
        self.rpc_times = rpc_times
        self.confirm_info = confirm_info

    def run(self):
        try:
            client = RpcClient(self.nodes[self.i])
            txs = []
            for i in range(self.tx_n):
                addr = client.rand_addr()
                tx_gas = client.DEFAULT_TX_GAS + 4 * self.tx_data_len
                tx = client.new_tx(receiver=addr, nonce=10000+i, value=0, gas=tx_gas, data=b'\x00' * self.tx_data_len)
                # remove big data field and assemble on full node to reduce network load.
                tx.__dict__["data"] = b''
                txs.append(tx)
            encoded_txs = eth_utils.encode_hex(rlp.encode(txs))

            start = time.time()
            h = self.nodes[self.i].test_generateblockwithfaketxs(encoded_txs, False, self.tx_data_len)
            self.confirm_info.add_block(h)
            self.rpc_times.append(round(time.time() - start, 3))
            self.log.debug("node %d actually generate block %s", self.i, h)
        except Exception as e:
            self.log.error("Node %d fails to generate block", self.i)
            self.log.error(str(e))


class SimpleGenerateThread(threading.Thread):
    def __init__(self, nodes, i, max_block_size, log, rpc_times:list, confirm_info: BlockConfirmationInfo):
        threading.Thread.__init__(self, daemon=True)
        self.nodes = nodes
        self.i = i
        self.max_block_size = max_block_size
        self.log = log
        self.rpc_times = rpc_times
        self.confirm_info = confirm_info

    def run(self):
        try:
            client = RpcClient(self.nodes[self.i])
            # Do not limit num tx in blocks, and block size is already limited by `max_block_size_in_bytes`
            start = time.time()
            h = client.generate_block(10000000, self.max_block_size)
            self.confirm_info.add_block(h)
            self.rpc_times.append(round(time.time() - start, 3))
            self.log.debug("node %d actually generate block %s", self.i, h)
        except Exception as e:
            self.log.error("Node %d fails to generate block", self.i)
            self.log.error(str(e))


if __name__ == "__main__":
    RemoteSimulate().main()
