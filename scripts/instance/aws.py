from botocore.exceptions import ClientError

import boto3, time
from typing import List

from .instance_config import (
    MAX_COUNT_IN_A_CALL,
    InstanceType,
    Region,
    Instance,
)


def launch_ec2_instance(
    region: Region,
    instance_types: List[InstanceType],
    key_name: str,
    role: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
) -> List[Instance]:
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
                    ec2,
                    type.name,
                    region.image,
                    key_name,
                    role,
                    zone.name,
                    zone.subnet,
                    region.security_group_id,
                    c,
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

    print(f"creating {len(all_instances)} instances")
    retry_count = 0
    while True:
        reservations = get_ec2_instance_information(ec2, role)
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
    instances = []
    for instance in all_instances:
        id_to_ips[instance["InstanceId"]] = instance["PublicIpAddress"]
        instances.append(
            Instance(
                instanceId=instance["InstanceId"],
                publicIpAddress=instance["PublicIpAddress"],
            )
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

    return instances_json, instances


def get_ec2_instance_information(ec2, role):
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


def terminate_ec2_instance(role, sampled, account, region):
    ec2 = boto3.client(
        "ec2",
        region_name=region.name,
        aws_access_key_id=account.access_key_id,
        aws_secret_access_key=account.access_key_secret,
    )

    reservations = get_ec2_instance_information(ec2, role)

    if reservations is not None:
        total = 0
        for reservation in reservations:
            instance_ids = []
            if len(sampled) > 0:
                for item in reservation["Instances"]:
                    ip = item["PrivateIpAddress"]
                    if ip not in sampled:
                        instance_ids.append(item["InstanceId"])
            else:
                instance_ids = list(
                    map(lambda x: x["InstanceId"], reservation["Instances"])
                )
            zone = ""
            instance_type = ""
            if len(instance_ids) > 0:
                zone = reservation["Instances"][0]["Placement"]["AvailabilityZone"]
                instance_type = reservation["Instances"][0]["InstanceType"]

            print(f"terminate instance: {instance_type} {zone} {len(instance_ids)}")
            total += len(instance_ids)

            for i in range(0, len(instance_ids), MAX_COUNT_IN_A_CALL):
                while True:
                    try:
                        response = ec2.terminate_instances(
                            InstanceIds=instance_ids[i : i + MAX_COUNT_IN_A_CALL]
                        )
                        break
                    except Exception as e:
                        print(f"Error terminate instance: {e}")

        print(f"total instances: {total}")
