import boto3, json, os, argparse, subprocess, time, shutil, re
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import List, Optional

MAX_COUNT_IN_A_CALL = 1000

# AvailabilityZone = [
#     ("us-west-2a", "subnet-a5cfe3dc"),
#     ("us-west-2b", "subnet-4d377e06"),
#     ("us-west-2c", "subnet-327d4368"),
#     ("us-west-2d", "subnet-b292b89a"),
# ]


@dataclass
class Zone:
    name: str
    subnet: Optional[str] = None


@dataclass
class Region:
    name: str
    image: str
    security_group_id: str
    count: int
    zones: List[Zone]


@dataclass
class InstanceType:
    name: str
    nodes: int


@dataclass
class AWSAccount:
    aws_access_key_id: str
    aws_secret_access_key: str
    regions: List[Region]
    type: List[InstanceType]


def from_dict(data: dict) -> AWSAccount:
    regions = [
        Region(
            name=r["name"],
            image=r["image"],
            security_group_id=r["security_group_id"],
            count=r["count"],
            zones=[Zone(**z) for z in r.get("zones", [])],
        )
        for r in data["regions"]
    ]

    types = [InstanceType(**t) for t in data["type"]]

    return AWSAccount(
        aws_access_key_id=data["aws_access_key_id"],
        aws_secret_access_key=data["aws_secret_access_key"],
        regions=regions,
        type=types,
    )


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-c", "--config", type=str, default="instance.cfg", help="configuration file"
    )
    parser.add_argument("-k", "--key", type=str, default="yuanl", help="key name id")
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )

    args = parser.parse_args()
    return args


# Create an EC2 instance
def create_ec2_instance(
    ec2,
    instance_type,
    image_id,
    key_name,
    role,
    zone,
    subnet_id,
    security_group_id,
    max_count=MAX_COUNT_IN_A_CALL,
):
    retry_count = 0
    while retry_count < 10:
        try:
            response = ec2.run_instances(
                ImageId=image_id,  # Replace with your desired AMI ID
                InstanceType=instance_type,  # Replace with your desired instance type
                MinCount=1,  # Minimum number of instances to launch
                MaxCount=max_count,  # Maximum number of instances to launch
                KeyName=key_name,  # Replace with your key pair name
                SecurityGroupIds=[
                    security_group_id
                ],  # Replace with your security group ID(s)
                # SubnetId=subnet_id,  # Replace with your subnet ID (optional)
                TagSpecifications=[  # Add tags to the instance
                    {
                        "ResourceType": "instance",
                        "Tags": [
                            {"Key": "role", "Value": role},
                            {"Key": "Name", "Value": f"{instance_type}-{image_id}"},
                        ],
                    },
                ],
                BlockDeviceMappings=[
                    {
                        "Ebs": {
                            "VolumeSize": 250,
                        },
                        "DeviceName": "/dev/sda1",
                    },
                ],
                Placement={
                    "AvailabilityZone": zone,
                },
            )

            print(
                f"{instance_type} {zone}: {len(response['Instances'])} instances created"
            )
            return response["Instances"]

        except ClientError as e:
            if e.response["Error"]["Code"] == "RequestLimitExceeded":
                print(
                    "Request limit for {}: {}, retry #{}".format(
                        instance_type, e.response["Error"]["Message"], retry_count
                    )
                )
                time.sleep(1.2**retry_count)
                retry_count += 1
                continue
            elif e.response["Error"]["Code"] == "ResourceCountExceeded":
                print(
                    "Request resource count exceeded for {}: {}, max cont {}".format(
                        instance_type, e.response["Error"]["Message"], max_count
                    )
                )
                return None
            else:
                print("Other error while creating {}: {}".format(instance_type, e))
                return None
        except Exception as e:
            print(f"Error creating instance: {e}")
            return None


def get_instance_information(ec2, role):
    try:
        # Describe EC2 instances
        response = ec2.describe_instances(
            Filters=[
                {"Name": "instance-state-name", "Values": ["running"]},
                {"Name": "tag:role", "Values": [role]},
            ]
        )

        return response["Reservations"]

    except Exception as e:
        print(f"Error retrieving instance information: {e}")
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
                        if x["PublicIpAddress"] in failure_ips
                    ]

                    new_success_instance = [
                        x
                        for x in wait_instances
                        if x["PublicIpAddress"] not in failure_ips
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
                ["./ip.sh", "--public", "--ip"],
                cwd=current_folder,
                stdout=f,
                stderr=subprocess.STDOUT,
                text=True,
            )

        if len(wait_instances) > 0:
            removed_ips = set(
                [(x["PublicIpAddress"], x["InstanceId"]) for x in wait_instances]
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
        file.write("\n".join(map(lambda x: x["InstanceId"], all_instances)))

    ips_file = os.path.join(current_folder, "ips")
    if os.path.isfile(ips_file):
        try:
            shutil.move(ips_file, os.path.join(current_folder, "ips_old"))
        except Exception as e:
            print(f"Error moving file: {e}")

    with open(ips_file, "w") as file:
        file.write("\n".join(map(lambda x: x["PublicIpAddress"], all_instances)))
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


def launch_instance(
    region: Region, instance_types: List[InstanceType], aws_access_key_id, aws_secret_access_key
):
    # Initialize a session using Amazon EC2
    ec2 = boto3.client(
        "ec2",
        region_name=region.name,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    total_count = region.count
    all_instances = []
    instances_json = {}
    for type in instance_types:
        node_per_host = type.nodes
        count = (total_count + node_per_host - 1) // node_per_host
        print(f"Creating {count} {type} instances")

        current_instances = []
        for zone in region.zones:
            while count > 0:
                c = min(count, MAX_COUNT_IN_A_CALL)
                instances = create_ec2_instance(
                    ec2, type.name, region.image, args.key, args.role, zone.name, zone.subnet, region.security_group_id, c
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
                instance_id = current_instance["InstanceId"]

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

    # write_instance(current_folder, all_instances)

    # write_instance_json(current_folder, instances_json)

    print(f"creating {len(all_instances)} instances")
    retry_count = 0
    while True:
        reservations = get_instance_information(ec2, args.role)
        running_instances = []
        if reservations is not None:
            for reservation in reservations:
                running_instances.extend(reservation["Instances"])

        print(f"{len(running_instances)} instances are running ...")
        if len(running_instances) == len(all_instances):
            all_instances = running_instances
            break

        retry_count += 1
        if retry_count >= 10:
            all_instances = running_instances

            break

        time.sleep(3)

    id_to_ips = {}
    for instance in all_instances:
        id_to_ips[instance["InstanceId"]] = instance["PublicIpAddress"]

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

    return instances_json, all_instances

    

if __name__ == "__main__":
    args = parse_args()

    current_folder = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_folder, args.config), "r") as f:
        raw_data = json.load(f)

    accounts = [from_dict(item) for item in raw_data]
    instances_json = {}
    all_instances = []
    for account in accounts:
        for region in account.regions:
            instances_j, instances = launch_instance(
                region,
                account.type,
                account.aws_access_key_id,
                account.aws_secret_access_key            )

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
            if line[-1] == '\n':
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

        # instances_json[k] = [x for x in v if x not in removed_ips]
        instances_json[k] = list(new_ips)
        total_count += len(instances_json[k])

    print(f"nodes in instances_json: {total_count}, nodes in ips: {len(ips)}")
    write_instance_json(current_folder, instances_json)


# def launch(
#     region: Region, instance_types: List[InstanceType], aws_access_key_id, aws_secret_access_key, security_group_id
# ):
#     # Initialize a session using Amazon EC2
#     ec2 = boto3.client(
#         "ec2",
#         region_name=region.name,
#         aws_access_key_id=aws_access_key_id,
#         aws_secret_access_key=aws_secret_access_key,
#     )

#     total_count = region.count
#     all_instances = []
#     instances_json = {}
#     for type in instance_types:
#         node_per_host = type.nodes
#         count = (total_count + node_per_host - 1) // node_per_host
#         print(f"Creating {count} {type} instances")

#         current_instances = []
#         for zone in region.zones:
#             while count > 0:
#                 c = min(count, MAX_COUNT_IN_A_CALL)
#                 instances = create_ec2_instance(
#                     ec2, type.name, region.image, args.key, args.role, zone.name, zone.subnet, c
#                 )
#                 time.sleep(5)
#                 if instances is not None:
#                     current_instances.extend(instances)
#                     count -= len(instances)
#                     if len(instances) < c:
#                         break
#                 else:
#                     break

#         if current_instances is not None:
#             all_instances.extend(current_instances)
#             for current_instance in current_instances:
#                 instance_id = current_instance["InstanceId"]

#                 if total_count >= node_per_host:
#                     total_count -= node_per_host
#                     if node_per_host in instances_json:
#                         instances_json[node_per_host].append(instance_id)
#                     else:
#                         instances_json[node_per_host] = [instance_id]
#                 else:
#                     if total_count in instances_json:
#                         instances_json[total_count].append(instance_id)
#                     else:
#                         instances_json[total_count] = [instance_id]
#                     total_count = 0

#             if total_count <= 0:
#                 break

#     assert total_count <= 0, f"Remaining instances need to create: {total_count}"

#     # write_instance(current_folder, all_instances)

#     # write_instance_json(current_folder, instances_json)

#     print(f"creating {len(all_instances)} instances")
#     retry_count = 0
#     while True:
#         reservations = get_instance_information(ec2, args.role)
#         running_instances = []
#         if reservations is not None:
#             for reservation in reservations:
#                 running_instances.extend(reservation["Instances"])

#         print(f"{len(running_instances)} instances are running ...")
#         if len(running_instances) == len(all_instances):
#             break

#         retry_count += 1
#         if retry_count >= 10:
#             all_instances = running_instances

#             break

#         time.sleep(3)

#     id_to_ips = {}
#     for instance in all_instances:
#         id_to_ips[instance["InstanceId"]] = instance["PublicIpAddress"]

#     for k, v in instances_json.items():
#         new_ips = set()
#         for id in v:
#             if id in id_to_ips:
#                 ip = id_to_ips[id]
#                 if ip not in new_ips:
#                     new_ips.add(ip)
#                 else:
#                     print(f"duplicate ip: {ip}, node {k}")
#             else:
#                 print(f"remove instance {id}, node {k}")

#         instances_json[k] = list(new_ips)

#     removed_ips = wait_for_instances_to_be_sshable(current_folder, all_instances)
#     print(f"removed {removed_ips}")
#     # removed_instance_ids = list(map(lambda x: x[1], removed_ips))
#     # for i in range(0, len(removed_instance_ids), MAX_COUNT_IN_A_CALL):
#     #     retries = 0
#     #     while retries < 3:
#     #         try:
#     #             response = ec2.terminate_instances(
#     #                 InstanceIds=removed_instance_ids[i : i + MAX_COUNT_IN_A_CALL]
#     #             )
#     #             break
#     #         except Exception as e:
#     #             print(f"Error terminate instance: {e}")

#     #         retries += 1

#     ips = set()
#     with open(os.path.join(current_folder, "ips"), "r") as ip_file:
#         for line in ip_file.readlines():
#             line = line[:-1]
#             ips.add(line)

#     total_count = 0
#     for k, v in instances_json.items():
#         new_ips = set()
#         for ip in v:
#             if ip in ips:
#                 if ip not in new_ips:
#                     new_ips.add(ip)
#                 else:
#                     print(f"duplicate ip {ip}, node {k}")
#             else:
#                 print(f"remove ip {ip}, node {k}")

#         # instances_json[k] = [x for x in v if x not in removed_ips]
#         instances_json[k] = list(new_ips)
#         total_count += len(instances_json[k])

#     print(f"nodes in instances_json: {total_count}, nodes in ips: {len(ips)}")
#     write_instance_json(current_folder, instances_json)
