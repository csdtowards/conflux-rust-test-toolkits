import json, os, subprocess, time, shutil, re
from typing import List

from instance_config import (
    load_config,
    parse_args,
    Instance,
)

from aws import launch_ec2_instance
from aliyun import launch_aliyun_instance


# Main function
def wait_for_instances_to_be_sshable(current_folder, all_instances: List[Instance]):
    print("Back up ~/.ssh/known_hosts to ./known_hosts_backup")
    known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")
    known_hosts_backup = os.path.expanduser("~/.ssh/known_hosts_backup")
    if os.path.exists(known_hosts_file):
        os.rename(known_hosts_file, known_hosts_backup)

    ips_log = os.path.join(current_folder, "ips.log")
    if os.path.isfile(ips_log):
        os.remove(ips_log)

    wait_instances = all_instances
    success_instances = []
    removed_ips = set()
    try:
        retry_count = 0
        while True:
            print("Wait for launched instances to be SSH-able")
            # Remove and recreate ~/.ssh/known_hosts
            if os.path.exists(known_hosts_file):
                os.remove(known_hosts_file)

            open(known_hosts_file, "w").close()

            failure_pattern = r"ssh: connect to host (\d+\.\d+\.\d+\.\d+) port 22"

            if os.path.isfile(ips_log):
                with open(ips_log, "r") as f:
                    content = f.read()
                    failure_ips = set(re.findall(failure_pattern, content))
                    print(f"Failure IPs: {failure_ips}")
                    new_wait_instances = [
                        x for x in wait_instances if x.publicIpAddress in failure_ips
                    ]

                    new_success_instance = [
                        x
                        for x in wait_instances
                        if x.publicIpAddress not in failure_ips
                    ]

                    success_instances.extend(new_success_instance)
                    wait_instances = new_wait_instances
                    # removed_ips.update(failure_ips)
            else:
                wait_instances = all_instances

            print(f"Waiting instances {len(wait_instances)}")
            if len(wait_instances) == 0 or retry_count >= 3:
                break
            else:
                write_instance(current_folder, wait_instances)

            with open(ips_log, "w") as f:
                subprocess.run(
                    ["./ip.sh"],
                    cwd=current_folder,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

            # print(f"Found {num_hosts} instances. Waiting...")
            retry_count += 1
            time.sleep(5)  # Wait before retrying

        # print(f"Number of hosts in ~/.ssh/known_hosts: {num_hosts}")
        write_instance(current_folder, success_instances)
        with open(os.path.join(current_folder, "ips1.log"), "w") as f:
            subprocess.run(
                ["./ip.sh", "--public", "--ip"],
                cwd=current_folder,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )

        if len(wait_instances) > 0:
            removed_ips = set(
                [(x.publicIpAddress, x.instanceId) for x in wait_instances]
            )

    except Exception as e:
        print(f"Error wait for instances to be sshable: {e}")
        raise e
    finally:
        print("Restore known_hosts")
        if os.path.exists(known_hosts_backup):
            os.rename(known_hosts_backup, known_hosts_file)

    return removed_ips


def write_instance(current_folder, all_instances: List[Instance]):
    instance_file = os.path.join(current_folder, "instances")
    if os.path.isfile(instance_file):
        try:
            shutil.move(instance_file, os.path.join(current_folder, "instances_old"))
        except Exception as e:
            print(f"Error moving file: {e}")

    with open(instance_file, "w") as file:
        file.write("\n".join(map(lambda x: x.instanceId, all_instances)))

    ips_file = os.path.join(current_folder, "ips")
    if os.path.isfile(ips_file):
        try:
            shutil.move(ips_file, os.path.join(current_folder, "ips_old"))
        except Exception as e:
            print(f"Error moving file: {e}")

    with open(ips_file, "w") as file:
        file.write("\n".join(map(lambda x: x.publicIpAddress, all_instances)))
        file.write("\n")


def write_instance_json(current_folder, all_instances):
    instance_file = os.path.join(current_folder, "instances.json")
    if os.path.isfile(instance_file):
        try:
            shutil.move(
                instance_file, os.path.join(current_folder, "instances_old.json")
            )
        except Exception as e:
            print(f"Error moving file: {e}")

    with open(instance_file, "w") as file:
        json.dump(all_instances, file, indent=4)


if __name__ == "__main__":
    args = parse_args()

    current_folder = os.path.dirname(os.path.abspath(__file__))
    current_folder = os.path.join(current_folder, "..")
    cloud_config = load_config(os.path.join(current_folder, args.config))

    instances_json = {}
    all_instances = []
    for account in cloud_config.aws:
        for region in account.regions:
            if region.count <= 0:
                continue

            instances_j, instances = launch_ec2_instance(
                region,
                account.type,
                args.key,
                args.role,
                account.access_key_id,
                account.access_key_secret,
            )

            all_instances.extend(instances)

            for k, v in instances_j.items():
                if k in instances_json:
                    instances_json[k].extend(v)
                else:
                    instances_json[k] = v

    for account in cloud_config.aliyun:
        for region in account.regions:
            if region.count <= 0:
                continue

            instances_j, instances = launch_aliyun_instance(
                region,
                account.type,
                args.key,
                args.role,
                account.access_key_id,
                account.access_key_secret,
            )

            all_instances.extend(instances)

            for k, v in instances_j.items():
                if k in instances_json:
                    instances_json[k].extend(v)
                else:
                    instances_json[k] = v

    removed_ips = wait_for_instances_to_be_sshable(current_folder, all_instances)
    print(f"removed {removed_ips}")
    # removed_instance_ids = list(map(lambda x: x[1], removed_ips))
    # for i in range(0, len(removed_instance_ids), MAX_COUNT_IN_A_CALL):
    #     retries = 0
    #     while retries < 3:
    #         try:
    #             response = ec2.terminate_instances(
    #                 InstanceIds=removed_instance_ids[i : i + MAX_COUNT_IN_A_CALL]
    #             )
    #             break
    #         except Exception as e:
    #             print(f"Error terminate instance: {e}")

    #         retries += 1

    ips = set()
    with open(os.path.join(current_folder, "ips"), "r") as ip_file:
        for line in ip_file.readlines():
            if line[-1] == "\n":
                ips.add(line[:-1])
            else:
                ips.add(line)

    total_count = 0
    for k, v in instances_json.items():
        new_ips = set()
        for ip in v:
            if ip in ips:
                if ip not in new_ips:
                    new_ips.add(ip)
                else:
                    print(f"duplicate ip {ip}, node {k}")
            else:
                print(f"remove ip {ip}, node {k}")

        instances_json[k] = list(new_ips)
        total_count += len(instances_json[k])

    print(f"nodes in instances_json: {total_count}, nodes in ips: {len(ips)}")
    write_instance_json(current_folder, instances_json)
