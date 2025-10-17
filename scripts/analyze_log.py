import os, platform, time, sys, shutil
from pathlib import Path


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

def run(log_dir):
    print("log dir: {}".format(log_dir))
    execute("./copy_logs_expand.sh {} > log_expand.log".format(log_dir), 3, "copy logs")
    os.system("echo `ls {}/logs_tmp | wc -l` logs expand.".format(log_dir))

    print("Computing latencies ...")
    ts = int(log_dir.split('_')[-1])
    stat_log_file = "exp_stat_latency_{}.log".format(ts)

    os.system("echo ============================================================ >> {}".format(stat_log_file))

    tag = "ms_k_vms_nodes_{}".format(ts)
    print("begin to statistic relay latency ...")
    ret = os.system("python3 stat_latency.py {0} {2} {0}.csv >> {1}".format(tag, stat_log_file, log_dir))
    assert ret == 0, "Failed to statistic block relay latency, return code = {}".format(ret)
    shutil.move(stat_log_file, "tmp")
    shutil.move("{}.csv".format(tag), "tmp")


def main():
    logs = sys.argv[1]
    for item in os.listdir(logs):
        full_path = os.path.join(logs, item)
        if os.path.isdir(full_path):
            run(os.path.join(logs, item))
            shutil.rmtree(full_path)
            time.sleep(1)

if __name__ == "__main__":
    main()