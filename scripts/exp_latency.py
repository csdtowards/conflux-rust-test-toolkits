#!/usr/bin/env python3

# allow imports from parent directory
# source: https://stackoverflow.com/a/11158224
import os, sys, re, json, time
sys.path.insert(1, os.path.join(sys.path[0], '..'))
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
import shutil
import threading

import argparse
from remote_simulate import RemoteSimulate, pssh, kill_remote_conflux, execute
import subprocess
from test_framework.test_framework import OptionHelper

def cleanup_remote_logs(ips_file:str):
    ret = pssh(ips_file, "rm -f *.tgz *.out; rm -rf /tmp/conflux_test_*", 1, "cleanup remote logs", "> cleanuplogs 2>&1")
    if ret > 0:
        failure_pattern = r"\[FAILURE\] (\d+\.\d+\.\d+\.\d+)"
        with open("cleanuplogs", "r") as f:
            content = f.read()
            failure_ips = set(re.findall(failure_pattern, content))
            print(f"Failure IPs: {failure_ips}")
            for ip in failure_ips:
                cmd = f'ssh -o "StrictHostKeyChecking no" {ip} "rm -f *.tgz *.out; rm -rf /tmp/conflux_test_*" > /dev/null 2>&1'
                if execute(cmd, 5, "cleanup remote logs") > 0:
                    print(f"Failed to cleanup remote logs {ip}")

def setup_bandwidth_limit(ips_file:str, bandwidth: float, nodes_per_host: int):
    cmd = f"./throttle_bitcoin_bandwidth.sh {bandwidth} {nodes_per_host}"
    ret = pssh(ips_file, cmd, 1, "setup bandwidth limit", "> bandwidthlimit 2>&1")
    if ret > 0:
        failure_pattern = r"\[FAILURE\] (\d+\.\d+\.\d+\.\d+)"
        with open("bandwidthlimit", "r") as f:
            content = f.read()
            failure_ips = set(re.findall(failure_pattern, content))
            print(f"Failure IPs: {failure_ips}")
            for ip in failure_ips:
                new_cmd = f'ssh -o "StrictHostKeyChecking no" {ip} "{cmd}" > /dev/null 2>&1'
                if execute(new_cmd, 5, "setup bandwidth limit") > 0:
                    print(f"Failed to setup bandwidth limit on {ip}")

class RemoteSimulateConfig:
    def __init__(self, block_gen_interval_ms, txs_per_block, tx_size, num_blocks):
        self.block_gen_interval_ms = block_gen_interval_ms
        self.txs_per_block = txs_per_block
        self.tx_size = tx_size
        self.num_blocks = num_blocks

    def __str__(self):
        return str(self.__dict__)

    @staticmethod
    def parse(batch_config):
        config_groups = []
        if batch_config[-1] == ",":
            # Ignore trailing comma
            batch_config = batch_config[:-1]
        for config in batch_config.split(","):
            fields = config.split(":")
            if len(fields) != 4:
                raise AssertionError("invalid config, format is <block_gen_interval_ms>:<txs_per_block>:<tx_size>:<num_blocks>")
            config_groups.append(RemoteSimulateConfig(
                int(fields[0]),
                int(fields[1]),
                int(fields[2]),
                int(fields[3]),
            ))

        return config_groups

class LatencyExperiment:
    def __init__(self):
        self.exp_name = "latency_latest"
        self.stat_confirmation_latency = False
        self.simulate_log_file = "exp.log"
        self.stat_log_file = "exp_stat_latency.log"
        self.stat_archive_file = "exp_stat_latency.tgz"

        parser = argparse.ArgumentParser(usage="%(prog)s [options]")
        self.exp_latency_options = dict(
            vms = 10,
            batch_config = "500:1:150000:1000,500:1:200000:1000,500:1:250000:1000,500:1:300000:1000,500:1:350000:1000",
            slave_role = "",
            old_tx_digest = False,
            terminate_instance = False,
        )
        OptionHelper.add_options(parser, self.exp_latency_options)

        def k_from_kv(kv):
            (k, v) = kv
            return k

        remote_simulate_options = dict(filter(
            lambda kv: k_from_kv(kv) in set(["bandwidth", "profiler", "enable_tx_propagation", "ips_file", "ips_file_sample", "enable_flamegraph"]),
            list(RemoteSimulate.SIMULATE_OPTIONS.items())))
        remote_simulate_options.update(RemoteSimulate.PASS_TO_CONFLUX_OPTIONS)
        # Configs with different default values than RemoteSimulate
        remote_simulate_options["nodes_per_host"] = 1
        remote_simulate_options["storage_memory_gb"] = 2
        remote_simulate_options["connect_peers"] = 8
        remote_simulate_options["tps"] = 4000

        OptionHelper.add_options(parser, remote_simulate_options)
        self.options = parser.parse_args()

        if self.options.old_tx_digest:
            self.options.tps *= 2

        if os.path.getsize("./genesis_secrets.txt") % 65 != 0:
            print("genesis secrets account error, file size should be multiple of 65")
            exit()
        self.options.txgen_account_count = int((os.path.getsize("./genesis_secrets.txt")/65) //
                                               (self.options.vms * self.options.nodes_per_host))

    def run(self):
        for config in RemoteSimulateConfig.parse(self.options.batch_config):
            print("=========================================================")
            print("Experiment started, config = {} ...".format(config))
            
            print("kill remote conflux and cleanup logs ...")
            # kill_remote_conflux(self.options.ips_file)
            # cleanup_remote_logs(self.options.ips_file)
            # setup_bandwidth_limit(self.options.ips_file, self.options.bandwidth, self.options.nodes_per_host)

            print("Run remote simulator ...")
            self.run_remote_simulate(config)

            print("Kill remote conflux and copy logs ...")
            # self.copy_remote_logs_1b1r()

            if self.options.terminate_instance:
                terminate_thread = threading.Thread(target=self.early_terminate)
                terminate_thread.start()
                
            kill_remote_conflux(self.options.ips_file_sample)
            self.copy_remote_logs()
            # Do not cleanup logs here because they may be needed for debug later, and they will be deleted when the
            # next run begins
            # cleanup_remote_logs(self.options.ips_file)

            os.makedirs("tmp", exist_ok=True)

            tag = self.tag(config)
            print(f"Collecting metrics ..., tag {tag}")
            ts = int(time.time())
            new_tag = "{}_{}".format(tag, int(ts))
            execute("./copy_file_from_slave.sh metrics.log {} > /dev/null".format(tag), 3, "collect metrics")
            execute("./copy_file_from_slave.sh conflux.log {} > /dev/null".format(tag), 3, "collect rust log")
            if self.options.enable_flamegraph:
                try:
                    execute("./copy_file_from_slave.sh conflux.svg {} > /dev/null".format(new_tag), 10, "collect flamegraph")
                except:
                    print("Failed to copy flamegraph file conflux.svg, please try again via copy_file_from_slave.sh in manual")

            if self.options.terminate_instance:
                self.terminate_instance()

            terminate_thread.join()
            shutil.move("logs", os.path.join("tmp", "logs_{}".format(ts)))
            fileName = "{}.metrics.log".format(tag)
            with open(fileName, "r") as f:
                lines = f.readlines()
                allNetworkSystemData =  []
                allNetworkConnectionData = []
                for i in range(len(lines)):
                    line = lines[i]
                    # if networkSystemData is None:
                    idx = line.find('network_system_data, Group, ')
                    if idx != -1:
                        s = line[idx + len('network_system_data, Group, '):]
                        s = re.sub(r'\b([a-zA-Z_\.\d]+): ([\d\.]+[,\n}])', r'"\1":\2', s)
                        s = s.replace(" ", "")
                        if "write.m1" in json.loads(s):
                            allNetworkSystemData.append((i, s))
                    # if networkConnectionData is None:
                    idx = line.find("network_connection_data, Group, ")
                    if idx != -1:
                        s = line[idx + len('network_connection_data, Group, '):]
                        s = re.sub(r'\b([a-zA-Z_\.\d]+): ([\d\.]+[,\n}])', r'"\1":\2', s)
                        s = s.replace(" ", "")
                        t = json.loads(s)
                        if (
                            "get_block_txn_response.m1" in t
                            or "get_block_txn_response_send_bytes.m1" in t
                            or "get_transactions_response.m1" in t
                            or "get_transactions_response_send_bytes.m1" in t
                            or "transactions.m1" in t
                            or "transactions_send_bytes.m1" in t
                        ):
                            allNetworkConnectionData.append((i, s))
                    # if networkConnectionData is None:
                    idx = line.find("p2p_events, Group, ")
                    if idx != -1:
                        s = line[idx + len('p2p_events, Group, '):]
                        s = re.sub(r'\b([a-zA-Z_\.\d]+): ([\d\.]+[,\n}])', r'"\1":\2', s)
                        s = s.replace(" ", "")
                        t = json.loads(s)
                        if (
                            "get_block_txn_response.m1" in t
                            or "get_block_txn_response_send_bytes.m1" in t
                            or "get_transactions_response.m1" in t
                            or "get_transactions_response_send_bytes.m1" in t
                            or "transactions.m1" in t
                            or "transactions_send_bytes.m1" in t
                        ):
                            allNetworkConnectionData.append((i, s))
                    # if networkSystemData and networkConnectionData:
                    #     break
                
                networkSystemData = None
                networkConnectionData = None
                networkSystemDataLast = None
                networkConnectionDataLast = None
                if len(allNetworkSystemData) > 0 and len(allNetworkConnectionData) > 0:
                    idx1 = len(allNetworkSystemData)//2
                    idx2 = len(allNetworkConnectionData)//2
                    line1, _ = allNetworkSystemData[idx1]
                    line2, _ = allNetworkConnectionData[idx2]
                    line = max(line1, line2)
                   
                    _, networkSystemData = min(allNetworkSystemData, key=lambda x: abs(x[0] - line))
                    _, networkConnectionData = min(allNetworkConnectionData, key=lambda x: abs(x[0] - line))

                    _, networkSystemDataLast = allNetworkSystemData[-1]
                    _, networkConnectionDataLast = allNetworkConnectionData[-1]
                elif len(allNetworkSystemData) > 0:
                    networkSystemData = allNetworkSystemData[len(allNetworkSystemData)//2]
                    _, networkSystemDataLast = allNetworkSystemData[-1]
                elif len(allNetworkConnectionData) > 0:
                    networkConnectionData = allNetworkConnectionData[len(allNetworkConnectionData)//2]
                    _, networkConnectionDataLast = allNetworkConnectionData[-1]

                redundancy = 0
                if networkConnectionData is not None and networkSystemData is not None:
                    # network_connection_data
                    a = json.loads(networkConnectionData)
                    # network_system_data
                    b = json.loads(networkSystemData)
                    get_block_txn_response = None
                    get_transactions_response = None
                    transactions = None
                    if get_block_txn_response is None:
                        if "get_block_txn_response.m1" in a:
                            get_block_txn_response = a["get_block_txn_response.m1"]

                    if get_block_txn_response is None:
                        if "get_block_txn_response_send_bytes.count" in a:
                            get_block_txn_response = a["get_block_txn_response_send_bytes.count"]
                        else:
                            get_block_txn_response = 0
                            
                    if get_transactions_response is None:
                        if "get_transactions_response.m1" in a:
                            get_transactions_response = a["get_transactions_response.m1"]

                    if get_transactions_response is None:
                        if "get_transactions_response_send_bytes.count" in a:
                            get_transactions_response = a["get_transactions_response_send_bytes.count"]
                        else:
                            get_transactions_response = 0

                    if transactions is None:
                        if "transactions.m1" in a:
                            transactions = a["transactions.m1"]

                    if transactions is None:
                        if "transactions_send_bytes.count" in a:
                            transactions = a["transactions_send_bytes.count"]
                        else:
                            transactions = 0

                    a1 = json.loads(networkConnectionDataLast)
                    b1 = json.loads(networkSystemDataLast)
                    last_line_counter = 0

                    if "get_block_txn_response_send_bytes.count" in a1:
                        last_line_counter += a1["get_block_txn_response_send_bytes.count"]

                    if "get_transactions_response_send_bytes.count" in a1:
                        last_line_counter += a1["get_transactions_response_send_bytes.count"]
                                        
                    if "write.count" in b and "write.count" in b1:
                        denominator = 1
                        print("old_tx_digest: {}".format(self.options.old_tx_digest))
                        if self.options.old_tx_digest:
                            denominator = 8

                        mid_counter = get_block_txn_response + get_transactions_response + transactions
                        print("last line counter: {}, mid counter: {}".format(last_line_counter, mid_counter))
                        print("last line write counter: {}, mid write count: {}".format(b1["write.count"], b["write.count"]))
                        redundancy = 1 - ((last_line_counter - mid_counter)/denominator)/ (b1["write.count"] - b["write.count"])
                        # redundancy = 1 - ((get_block_txn_response + get_transactions_response + transactions))/ b["write.m1"]
                        print("get_block_txn_response + get_transactions_response + transactions: {}".format(get_block_txn_response + get_transactions_response + transactions))
                        print("b[write.count]: {}".format(b["write.count"]))
                os.system("echo TX redundancy: {} >> {}".format(redundancy, "exp.log"))

            # execute("cp exp.log {}.exp.log".format(new_tag), 3, "copy exp.log")
            shutil.move("exp.log", os.path.join("tmp", "{}.exp.log".format(new_tag)))
            shutil.move("{}.conflux.log".format(tag), os.path.join("tmp", "{}.conflux.log".format(new_tag)))
            shutil.move("{}.metrics.log".format(tag), os.path.join("tmp", "{}.metrics.log".format(new_tag)))
            # if self.options.terminate_instance or not self.options.terminate_instance:
            #     return
            
            # self.expand_logs()

            # print("Statistic logs ...")
            # os.system("echo throttling logs: `grep -i thrott -r logs | wc -l`")
            # # os.system("echo error logs: `grep -i thrott -r logs | wc -l`")

            # print("Computing latencies ...")
            # self.stat_latency(config)

            # execute("cp exp.log {}.exp.log".format(tag), 3, "copy exp.log")


        # cmd = "tar cvfz {} {} *.exp.log *nodes.csv *.metrics.log *.conflux.log".format(self.stat_archive_file, self.stat_log_file)
        # if self.options.enable_flamegraph:
        #     cmd = cmd + " *.conflux.svg"
        # os.system(cmd)
        if self.options.terminate_instance:
            print("begin to statistic relay latency ...")
            ret = os.system("python3 analyze_log.py {0}".format("tmp"))
        
            print("=========================================================")
            print("archive the experiment results into [{}] ...".format(self.stat_archive_file))
            cmd = "tar cvfz {} {}".format(self.stat_archive_file, "tmp")
            os.system(cmd)
        
        # cmd = "tar cvfz logs_metrics.tgz -C logs/ logs_metrics/"
        # os.system(cmd)

        # cmd = "find logs_tmp/ -type f -name 'conflux.new_block_ready.log' | tar cvfz new_block_ready.tgz -T -"
        # original_dir = os.getcwd()
        # os.chdir('logs')
        # os.system(cmd)
        # os.chdir(original_dir)

        # cmd = "tar cvf logs_1b1r.tgz -C logs/ logs_1b1r/"
        # os.system(cmd)
        
    def early_terminate(self):
        ips = set()
        with open(self.options.ips_file, 'r') as ip_file:
            for line in ip_file.readlines():
                line = line[:-1]
                ips.add(line)

        ips_in_use = set()
        with open(self.options.ips_file_sample, 'r') as ip_file:
            for line in ip_file.readlines():
                line = line[:-1]
                ips_in_use.add(line)

        if len(ips.difference(ips_in_use)) > 0:
            self.terminate_instance(no_log=True)
        
    def terminate_instance(self, no_log=False):
        cmd = []
        if no_log:
            cmd = [
                    "python3",
                    "./instance/terminate-on-demand-region.py",
                    "--role", self.options.slave_role,
                    "--sample",
                ]
        else:
            cmd = [
                    "python3",
                    "./instance/terminate-on-demand-region.py",
                    "--role", self.options.slave_role,
                ]
        log_file = open(self.simulate_log_file, "a")
        print("[CMD]: {} >> {}".format(cmd, self.simulate_log_file))
        ret = subprocess.run(cmd, stdout = log_file, stderr=log_file).returncode
        if ret != 0:
            print("Failed to terminate instance, return code = {}. Please check [{}] for more details".format(ret, self.simulate_log_file))

    def copy_remote_logs(self):
        execute("./copy_logs.sh > log_copy.log", 3, "copy logs")
        os.system("echo `ls logs/logs_tmp | wc -l` logs copied.")

    def copy_remote_logs_1b1r(self):
        execute("./copy_logs_1b1r.sh > log_copy.log", 3, "copy logs")
        os.system("echo `ls logs/logs_1b1r | wc -l` logs copied.")
        
    def expand_logs(self):
        execute("./copy_logs_expand.sh logs > log_expand.log", 3, "copy logs")
        os.system("echo `ls logs/logs_tmp | wc -l` logs expand.")

    def run_remote_simulate(self, config:RemoteSimulateConfig):
        cmd = [
            "python3",
            "../remote_simulate.py",
            "--generation-period-ms", str(config.block_gen_interval_ms),
            "--num-blocks", str(config.num_blocks),
            "--txs-per-block", str(config.txs_per_block),
            "--generate-tx-data-len", str(config.tx_size),
            "--tx-pool-size", str(1_000_000),
            "--conflux-binary", "~/conflux",
            "--nocleanup"
        ] + OptionHelper.parsed_options_to_args(
            dict(filter(lambda kv: kv[0] not in self.exp_latency_options, vars(self.options).items()))
        )

        log_file = open(self.simulate_log_file, "a")
        print("[CMD]: {} >> {}".format(cmd, self.simulate_log_file))
        ret = subprocess.run(cmd, stdout = log_file, stderr=log_file).returncode
        assert ret == 0, "Failed to run remote simulator, return code = {}. Please check [{}] for more details".format(ret, self.simulate_log_file)

        os.system('grep "(ERROR)" {}'.format(self.simulate_log_file))

    def tag(self, config:RemoteSimulateConfig):
        block_size_kb = config.txs_per_block * config.tx_size // 1000
        return "{}ms_{}k_{}vms_{}nodes".format(
            config.block_gen_interval_ms,
            block_size_kb,
            self.options.vms,
            self.options.nodes_per_host,
        )

    def stat_latency(self, config:RemoteSimulateConfig):
        os.system("echo ============================================================ >> {}".format(self.stat_log_file))

        print("begin to statistic relay latency ...")
        ret = os.system("python3 stat_latency.py {0} logs {0}.csv >> {1}".format(self.tag(config), self.stat_log_file))
        assert ret == 0, "Failed to statistic block relay latency, return code = {}".format(ret)

        if self.stat_confirmation_latency:
            print("begin to statistic confirmation latency ...")
            ret = os.system("python3 stat_confirmation.py logs 4 >> {}".format(self.stat_log_file))
            assert ret == 0, "Failed to statistic block confirmation latency, return code = {}".format(ret)


if __name__ == "__main__":
    LatencyExperiment().run()
