import json, os, argparse, subprocess, time, shutil, re

from Tea.exceptions import UnretryableException
from alibabacloud_ecs20140526.client import Client as Ecs20140526Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526 import models as ecs_20140526_models
from alibabacloud_tea_util import models as util_models

# from alibabacloud_tea_util.client import Client as UtilClient

# from alibabacloud_credentials.client import Client as CredClient
# from alibabacloud_tea_rpc.models import Config

REGION_ID = "us-east-1"

# credentialsClient = CredClient()
# config = Config(credential=credentialsClient)
config = open_api_models.Config(
    access_key_id=os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
    access_key_secret=os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
)
config.endpoint = f"ecs.us-east-1.aliyuncs.com"
client = Ecs20140526Client(config)

MAX_COUNT_IN_A_CALL = 1000

AvailabilityZone = [
    ("us-east-1a", "vsw-0xicmmybynpkbo68t6xai"),
    ("us-east-1b", "vsw-0xiamapbe0bmqfhuwxscz"),
    # ("us-west-2c", "subnet-327d4368"),
    # ("us-west-2d", "subnet-b292b89a"),
]


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-c", "--config", type=str, default="instance.cfg", help="configuration file"
    )
    parser.add_argument("-s", "--slave", type=int, default="2", help="slave count")
    parser.add_argument(
        "-i", "--image", type=str, default="m-0xibhstptnimrrvcvutc", help="image id"
    )
    parser.add_argument("-k", "--key", type=str, default="yuanl", help="key name id")
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )
    args = parser.parse_args()
    return args


# Create an EC2 instance
def create_ec2_instance(
    instance_type,
    image_id,
    key_name,
    role,
    zone,
    subnet_id,
    max_count=MAX_COUNT_IN_A_CALL,
):
    retry_count = 0
    while retry_count < 10:
        run_instances_request = ecs_20140526_models.RunInstancesRequest(
            region_id=REGION_ID,
            image_id=image_id,
            instance_type=instance_type,
            min_amount=1,
            amount=max_count,
            key_pair_name=key_name,
            security_group_id="sg-0xi2ymnbkmebbeztohrb",
            v_switch_id=subnet_id,
            tag=[
                ecs_20140526_models.RunInstancesRequestTag(key="role", value=role),
                ecs_20140526_models.RunInstancesRequestTag(
                    key="Name", value=f"{instance_type}-{image_id}"
                ),
            ],
            system_disk=ecs_20140526_models.RunInstancesRequestSystemDisk(
                size="250", category="cloud_essd"
            ),
            zone_id=zone,
        )
        runtime = util_models.RuntimeOptions()
        try:
            response = client.run_instances_with_options(run_instances_request, runtime)
            instance_ids = response.body.instance_id_sets.instance_id_set
            print(f"{instance_type} {zone}: {len(instance_ids)} instances created")
            return instance_ids
        except UnretryableException as e:
            if e.message.find("connect timeout") != -1:
                print(
                    "Request timeout for {}: {}, retry #{}".format(
                        instance_type, e, retry_count
                    )
                )
                time.sleep(1.2**retry_count)
                retry_count += 1
                continue
            else:
                print("Other error while creating {}: {}".format(instance_type, e))
                return None
        except Exception as error:
            print(error.message)
            print(error.data.get("Recommend"))
            return None


def get_instance_information(role):
    describe_instances_request = ecs_20140526_models.DescribeInstancesRequest(
        region_id="us-east-1",
        status="Running",
        tag=[ecs_20140526_models.DescribeInstancesRequestTag(key="role", value=role)],
    )
    runtime = util_models.RuntimeOptions()
    try:
        response = client.describe_instances_with_options(
            describe_instances_request, runtime
        )
        return response.body
    except Exception as error:
        print(error.message)
        print(error.data.get("Recommend"))
        return None


# Main function
def wait_for_instances_to_be_sshable(current_folder, all_instances):
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
                        x
                        for x in wait_instances
                        if x.vpc_attributes.private_ip_address.ip_address[0]
                        in failure_ips
                    ]

                    new_success_instance = [
                        x
                        for x in wait_instances
                        if x.vpc_attributes.private_ip_address.ip_address[0]
                        not in failure_ips
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

            # Check if the number of hosts in known_hosts matches the expected number
            # with open(known_hosts_file, "r") as f:
            #     num_hosts = sum(1 for _ in f)

            # if num_hosts == len(all_instances):
            #     print(f"All {num_hosts} instances are SSH-able.")
            #     break

            # print(f"Found {num_hosts} instances. Waiting...")
            retry_count += 1
            time.sleep(5)  # Wait before retrying

        # print(f"Number of hosts in ~/.ssh/known_hosts: {num_hosts}")
        write_instance(current_folder, success_instances)
        with open(os.path.join(current_folder, "ips1.log"), "w") as f:
            subprocess.run(
                ["./ip.sh", "--private", "--ip"],
                cwd=current_folder,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )

        if len(wait_instances) > 0:
            removed_ips = set(
                [
                    (x.vpc_attributes.private_ip_address.ip_address[0], x.instance_id)
                    for x in wait_instances
                ]
            )

    except Exception as e:
        print(f"Error wait for instances to be sshable: {e}")
        raise e
    finally:
        print("Restore known_hosts")
        if os.path.exists(known_hosts_backup):
            os.rename(known_hosts_backup, known_hosts_file)

    return removed_ips


def write_instance(current_folder, all_instances):
    instance_file = os.path.join(current_folder, "instances")
    if os.path.isfile(instance_file):
        try:
            shutil.move(instance_file, os.path.join(current_folder, "instances_old"))
        except Exception as e:
            print(f"Error moving file: {e}")

    with open(instance_file, "w") as file:
        file.write("\n".join(map(lambda x: x.instance_id, all_instances)))


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
    with open(os.path.join(current_folder, args.config), "r") as f:
        config = json.load(f)

    total_count = args.slave
    instance_types = config["type"]
    all_instances = []
    instances_json = {}
    for type in instance_types:
        node_per_host = type["nodes"]
        count = (total_count + node_per_host - 1) // node_per_host
        print(f"Creating {count} {type} instances")

        current_instances = []
        for zone, subnet in AvailabilityZone:
            while count > 0:
                c = min(count, MAX_COUNT_IN_A_CALL)
                instances = create_ec2_instance(
                    type["name"], args.image, args.key, args.role, zone, subnet, c
                )
                time.sleep(5)
                if instances is not None:
                    current_instances.extend(instances)
                    count -= len(instances)
                    if len(instances) < c:
                        break
                else:
                    break

        if current_instances is not None:
            all_instances.extend(current_instances)
            for current_instance in current_instances:
                instance_id = current_instance

                if total_count >= node_per_host:
                    total_count -= node_per_host
                    if node_per_host in instances_json:
                        instances_json[node_per_host].append(instance_id)
                    else:
                        instances_json[node_per_host] = [instance_id]
                else:
                    if total_count in instances_json:
                        instances_json[total_count].append(instance_id)
                    else:
                        instances_json[total_count] = [instance_id]
                    total_count = 0

            if total_count <= 0:
                break

    assert total_count <= 0, f"Remaining instances need to create: {total_count}"

    print(f"creating {len(all_instances)} instances")
    retry_count = 0
    while True:
        running_instances = get_instance_information(args.role)

        print(f"{running_instances.total_count} instances are running ...")
        if running_instances.total_count == len(all_instances):
            all_instances = running_instances.instances.instance
            break

        retry_count += 1
        if retry_count >= 10:
            all_instances = running_instances.instances.instance

            break

        time.sleep(3)

    id_to_ips = {}
    for instance in all_instances:
        id_to_ips[instance.instance_id] = (
            instance.vpc_attributes.private_ip_address.ip_address[0]
        )

    for k, v in instances_json.items():
        new_ips = set()
        for id in v:
            if id in id_to_ips:
                ip = id_to_ips[id]
                if ip not in new_ips:
                    new_ips.add(ip)
                else:
                    print(f"duplicate ip: {ip}, node {k}")
            else:
                print(f"remove instance {id}, node {k}")

        instances_json[k] = list(new_ips)

    removed_ips = wait_for_instances_to_be_sshable(current_folder, all_instances)
    print(f"removed {removed_ips}")
    removed_instance_ids = list(map(lambda x: x[1], removed_ips))
    for i in range(0, len(removed_instance_ids), MAX_COUNT_IN_A_CALL):
        retries = 0
        while retries < 3:
            try:
                response = client.delete_instance_with_options(
                    ecs_20140526_models.DeleteInstancesRequest(
                        region_id=REGION_ID,
                        instance_id=removed_instance_ids[i : i + MAX_COUNT_IN_A_CALL],
                        force=True,
                    ),
                    util_models.RuntimeOptions(),
                )
                break
            except Exception as e:
                print(f"Error terminate instance: {e}")

            retries += 1

    ips = set()
    with open(os.path.join(current_folder, "ips"), "r") as ip_file:
        for line in ip_file.readlines():
            line = line[:-1]
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

        # instances_json[k] = [x for x in v if x not in removed_ips]
        instances_json[k] = list(new_ips)
        total_count += len(instances_json[k])

    print(f"nodes in instances_json: {total_count}, nodes in ips: {len(ips)}")
    write_instance_json(current_folder, instances_json)
