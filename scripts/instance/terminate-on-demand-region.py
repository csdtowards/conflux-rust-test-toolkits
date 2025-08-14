from datetime import datetime
import os

from .instance_config import load_config, parse_args, MAX_COUNT_IN_A_CALL
from .aws import terminate_ec2_instance
from .aliyun import terminate_aliyun_instance


if __name__ == "__main__":
    args = parse_args()

    current_folder = os.path.dirname(os.path.abspath(__file__))
    cloud_config = load_config(os.path.join(current_folder, args.config))

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

    for account in cloud_config.aws:
        for region in account.regions:
            terminate_ec2_instance(args.role, sampled, account, region)

    for account in cloud_config.aliyun:
        for region in account.regions:
            terminate_aliyun_instance(args.role, sampled, account, region)

    # Print the response
    # print(response)
    now = datetime.now()
    print(f"Current date and time: {now}")
