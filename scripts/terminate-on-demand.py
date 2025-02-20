import boto3, argparse, os
from datetime import datetime


# Initialize a session using Amazon EC2
ec2 = boto3.client("ec2", region_name="us-west-2")  # Replace with your desired region

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
