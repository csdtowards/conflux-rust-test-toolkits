import time

from Tea.exceptions import UnretryableException
from alibabacloud_ecs20140526.client import Client as Ecs20140526Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_ecs20140526 import models as ecs_20140526_models
from alibabacloud_tea_util import models as util_models
from typing import List

from .instance_config import (
    MAX_COUNT_IN_A_CALL,
    InstanceType,
    Region,
    Instance,
)


def launch_aliyun_instance(
    region: Region,
    instance_types: List[InstanceType],
    key_name: str,
    role: str,
    access_key_id,
    access_key_secret,
) -> List[Instance]:
    config = open_api_models.Config(
        access_key_id=access_key_id,
        access_key_secret=access_key_secret,
        region_id=region.name,
        # endpoint=f"ecs.{region.name}.aliyuncs.com",
    )
    client = Ecs20140526Client(config)

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
                instances = create_aliyun_instance(
                    client,
                    region.name,
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
        running_instances = get_aliyun_instance_information(client, region.name, role)

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
    instances = []
    for instance in all_instances:
        id_to_ips[instance.instance_id] = instance.public_ip_address.ip_address

        instances.append(
            Instance(
                instanceId=instance.instance_id,
                publicIpAddress=instance.public_ip_address.ip_address,
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


def get_aliyun_instance_information(
    client: Ecs20140526Client, region_id: str, role: str
):
    describe_instances_request = ecs_20140526_models.DescribeInstancesRequest(
        region_id,
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


# Create an EC2 instance
def create_aliyun_instance(
    client: Ecs20140526Client,
    region_id: str,
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
        run_instances_request = ecs_20140526_models.RunInstancesRequest(
            region_id,
            image_id=image_id,
            instance_type=instance_type,
            min_amount=1,
            amount=max_count,
            key_pair_name=key_name,
            security_group_id=security_group_id,
            # v_switch_id=subnet_id,
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


def terminate_aliyun_instance(role, sampled, account, region):
    config = open_api_models.Config(
        access_key_id=account.access_key_id,
        access_key_secret=account.access_key_secret,
        region_id=region.name,
    )
    client = Ecs20140526Client(config)
    reservations = get_aliyun_instance_information(client, region.name, role)
    if reservations is not None:
        total = 0
        instances = reservations.instances.instance
        instance_ids = []
        if len(sampled) > 0:
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
                            region_id=region.name,
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
