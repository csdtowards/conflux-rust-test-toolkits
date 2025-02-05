import boto3, argparse


# Initialize a session using Amazon EC2
ec2 = boto3.client("ec2", region_name="us-west-2")  # Replace with your desired region

MAX_COUNT_IN_A_CALL = 1000


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )
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
    if reservations is not None:
        for reservation in reservations:
            instance_ids = list(
                map(lambda x: x["InstanceId"], reservation["Instances"])
            )
            zone = ""
            instance_type = ""
            if len(instance_ids) > 0:
                zone = reservation["Instances"][0]["Placement"]["AvailabilityZone"]
                instance_type = reservation["Instances"][0]["InstanceType"]

            print(f"terminate instance: {instance_type} {zone} {len(instance_ids)}")

            for i in range(0, len(instance_ids), MAX_COUNT_IN_A_CALL):
                while True:
                    try:
                        response = ec2.terminate_instances(
                            InstanceIds=instance_ids[i : i + MAX_COUNT_IN_A_CALL]
                        )
                        break
                    except Exception as e:
                        print(f"Error terminate instance: {e}")

    # Print the response
    # print(response)
