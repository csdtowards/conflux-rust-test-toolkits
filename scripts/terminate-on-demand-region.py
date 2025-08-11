from datetime import datetime
import boto3, json, os, argparse, subprocess, time, shutil, re
from botocore.exceptions import ClientError
from dataclasses import dataclass
from typing import List, Optional

MAX_COUNT_IN_A_CALL = 1000


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
        "-c", "--config", type=str, default="instance-region.cfg", help="configuration file"
    )
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )
    parser.add_argument("-s", "--sample", action="store_true", help="sample")
    args = parser.parse_args()
    return args


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


if __name__ == "__main__":
    args = parse_args()

    current_folder = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_folder, args.config), "r") as f:
        raw_data = json.load(f)

    accounts = [from_dict(item) for item in raw_data]

    sampled = set()
    if args.sample:
        current_folder = os.path.dirname(os.path.abspath(__file__))
        sample_ips = os.path.join(current_folder, "ips_sample")
        if os.path.exists(sample_ips):
            with open(sample_ips, "r") as file:
                lines = file.readlines()
                for line in lines:
                    l = line.strip()
                    if l != "":
                        sampled.add(l)
                        
    for account in accounts:
        for region in account.regions:
            ec2 = boto3.client(
                "ec2",
                region_name=region.name,
                aws_access_key_id=account.aws_access_key_id,
                aws_secret_access_key=account.aws_secret_access_key,
            )
            
            reservations = get_instance_information(ec2, args.role)

            if reservations is not None:
                total = 0
                for reservation in reservations:
                    instance_ids = []
                    if args.sample:
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

    # Print the response
    # print(response)
    now = datetime.now()
    print(f"Current date and time: {now}")
