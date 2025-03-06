import json, os, argparse, subprocess, time, shutil, re
from datetime import datetime

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


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )
    parser.add_argument("-s", "--sample", action="store_true", help="sample")
    args = parser.parse_args()
    return args


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


if __name__ == "__main__":
    args = parse_args()

    reservations = get_instance_information(args.role)

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

    if reservations is not None:
        total = 0
        instances = reservations.instances.instance
        instance_ids = []
        if args.sample:
            for item in instances:
                ip = item.vpc_attributes.private_ip_address.ip_address[0]
                if ip not in sampled:
                    instance_ids.append(item.instance_id)
        else:
            instance_ids = list(map(lambda x: x.instance_id, instances))

        zone = ""
        instance_type = ""
        if len(instance_ids) > 0:
            zone = instances[0].zone_id
            instance_type = instances[0].instance_type

        print(f"terminate instance: {instance_type} {zone} {len(instance_ids)}")
        total += len(instance_ids)

        for i in range(0, len(instance_ids), MAX_COUNT_IN_A_CALL):
            while True:
                try:
                    response = client.delete_instances_with_options(
                        ecs_20140526_models.DeleteInstancesRequest(
                            region_id=REGION_ID,
                            instance_id=instance_ids[i : i + MAX_COUNT_IN_A_CALL],
                            force=True,
                        ),
                        util_models.RuntimeOptions(),
                    )
                    break
                except Exception as e:
                    print(f"Error terminate instance: {e}")
                    time.sleep(1)

        print(f"total instances: {total}")

    now = datetime.now()
    print(f"Current date and time: {now}")
