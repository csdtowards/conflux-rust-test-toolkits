import boto3, json, os, argparse, subprocess, time, shutil, re
from botocore.exceptions import ClientError


# Initialize a session using Amazon EC2
ec2 = boto3.client("ec2", region_name="us-west-2")  # Replace with your desired region

MAX_COUNT_IN_A_CALL = 1000

AvailabilityZone = [
    ("us-west-2a", "subnet-a5cfe3dc"),
    ("us-west-2b", "subnet-4d377e06"),
    ("us-west-2c", "subnet-327d4368"),
    ("us-west-2d", "subnet-b292b89a"),
]


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-c", "--config", type=str, default="instance.cfg", help="configuration file"
    )
    parser.add_argument("-s", "--slave", type=int, default="4", help="slave count")
    parser.add_argument(
        "-i", "--image", type=str, default="ami-03b1df5a8a45b701f", help="image id"
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
        try:
            response = ec2.run_instances(
                ImageId=image_id,  # Replace with your desired AMI ID
                InstanceType=instance_type,  # Replace with your desired instance type
                MinCount=1,  # Minimum number of instances to launch
                MaxCount=max_count,  # Maximum number of instances to launch
                KeyName=key_name,  # Replace with your key pair name
                SecurityGroupIds=[
                    "sg-0345bbb6934681ea1"
                ],  # Replace with your security group ID(s)
                SubnetId=subnet_id,  # Replace with your subnet ID (optional)
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


def get_instance_information(role):
    try:
        # Describe EC2 instances
        response = ec2.describe_instances(
            Filters=[
                {"Name": "instance-state-name", "Values": ["running"]},
                {"Name": "tag:role", "Values": [role]},
            ]
        )

        # Loop through reservations and instances
        # for reservation in response['Reservations']:
        #     for instance in reservation['Instances']:
        #         instance_id = instance['InstanceId']
        #         instance_type = instance['InstanceType']
        #         instance_state = instance['State']['Name']
        #         public_ip = instance.get('PublicIpAddress', 'N/A')
        #         private_ip = instance.get('PrivateIpAddress', 'N/A')
        #         launch_time = instance['LaunchTime'].strftime('%Y-%m-%d %H:%M:%S')

        #         # Print instance information
        #         print(f"Instance ID: {instance_id}")
        #         print(f"Instance Type: {instance_type}")
        #         print(f"Instance State: {instance_state}")
        #         print(f"Public IP: {public_ip}")
        #         print(f"Private IP: {private_ip}")
        #         print(f"Launch Time: {launch_time}")
        #         print("-" * 40)

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
                        if x["PrivateIpAddress"] in failure_ips
                    ]

                    new_success_instance = [
                        x
                        for x in wait_instances
                        if x["PrivateIpAddress"] not in failure_ips
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
                [(x["PrivateIpAddress"], x["InstanceId"]) for x in wait_instances]
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
        reservations = get_instance_information(args.role)
        running_instances = []
        if reservations is not None:
            for reservation in reservations:
                running_instances.extend(reservation["Instances"])

        print(f"{len(running_instances)} instances are running ...")
        if len(running_instances) == len(all_instances):
            break

        retry_count += 1
        if retry_count >= 10:
            all_instances = running_instances

            break

        time.sleep(3)

    id_to_ips = {}
    for instance in all_instances:
        id_to_ips[instance["InstanceId"]] = instance["PrivateIpAddress"]

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
                response = ec2.terminate_instances(
                    InstanceIds=removed_instance_ids[i : i + MAX_COUNT_IN_A_CALL]
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
