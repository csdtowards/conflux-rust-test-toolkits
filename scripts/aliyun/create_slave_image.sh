#!/usr/bin/env bash
set -u

SCRIPT_DIR="$( cd "$(dirname "$0")"/.. ; pwd -P )"

# if [[ $# -lt 1 ]]; then
#     echo "Parameters required: <keypair> [<branch>] [<repo>]"
#     exit 1
# fi

key_pair="${1:-yuanl}"
branch="${2:-block_event_record_without_sign}"
repo="${3:-https://github.com/csdtowards/conflux-rust}"

type="ecs.g8i.2xlarge"
role="${key_pair}_master"
image="m-0xicmwdj236qs7osfal6"

# create instance
echo "create an instance to make slave image ..."
res=`aliyun ecs RunInstances --RegionId us-east-1 --ImageId $image --Amount 1 --InstanceType $type --KeyPairName $key_pair --InternetMaxBandwidthOut 100 --SecurityGroupId sg-0xi2ymnbkmebbeztohrb --VSwitchId vsw-0xiamapbe0bmqfhuwxscz --SystemDisk.Size 100 --SystemDisk.Category cloud_essd --Tag.1.Key role --Tag.1.Value $role --Tag.2.Key Name --Tag.2.Value $type-$image`
echo $res | jq ".InstanceIdSets.InstanceIdSet[]" | tr -d '"' > instances

num_created=`cat instances | wc -l`
if [ "$num_created" -ne "1" ]; then
    echo "not enough instances created, required $n, created $num_created"
    exit 1
fi
echo "1 instances created."

# wait for all instances in running state
while true
do
	instances=`aliyun ecs DescribeInstances --RegionId us-east-1 --Status Running --Tag.1.Key role --Tag.1.Value $role`
	num_runnings=`echo $instances | jq ".Instances.Instance[].InstanceId" | wc -l`
	echo "$num_runnings instances are running ..."
	if [[ 1 -le $num_runnings  ]]; then
		break
	fi
	sleep 3
done

echo "Wait for launched instances able to be connected"
touch ~/.ssh/known_hosts
echo "Back up ~/.ssh/known_hosts to ./known_hosts_backup"
mv ~/.ssh/known_hosts known_hosts_backup
# retrieve IPs and SSH all instances to update known_hosts
while true
do
    rm -f ~/.ssh/known_hosts
    touch ~/.ssh/known_hosts
    $SCRIPT_DIR/aliyun/ip.sh --public
    if [[ 1 -eq `cat ~/.ssh/known_hosts | wc -l`  ]]; then
        break
    fi
done
wc -l ~/.ssh/known_hosts
echo "Restore known_hosts"
mv known_hosts_backup ~/.ssh/known_hosts

echo "setup before making slave image ..."
master_ip=`cat ips`
master_id=`cat instances`
setup_script="setup_image.sh"
scp -o "StrictHostKeyChecking no" $SCRIPT_DIR/$setup_script ubuntu@$master_ip:~
echo "run $setup_script"
ssh -o "StrictHostKeyChecking no" -tt ubuntu@$master_ip ./$setup_script $branch $repo false

echo "stop instance ..."
aliyun ecs StopInstances --RegionId 'us-east-1' --InstanceId.1 $master_id

while true
do
	instances=`aliyun ecs DescribeInstances --RegionId us-east-1 --InstanceIds [\"$master_id\"]`
	status=`echo $instances | jq ".Instances.Instance[].Status" | tr -d '"'`
	echo "instances are $status ..."
	if [ "$status" == "Stopped" ]; then
		break
	fi
	sleep 3
done

# create slave image
echo "create slave image ..."
res=`aliyun ecs CreateImage --RegionId us-east-1 --InstanceId $master_id --ImageName ${key_pair}_slave_image`
image_id=`echo $res | jq ".ImageId" | tr -d '"'`
echo "slave image created: $image_id"

# wait until image is available
while true
do
    image_info=`aliyun ecs DescribeImages --RegionId us-east-1 --ImageId $image_id`
    image_status=`echo $image_info | jq ".Images.Image[].Status" | tr -d '"'`
    echo "image is $image_status"
    if [ "$image_status" != "" ] && [ "$image_status" != "Creating" ] && [ "$image_status" != "Waiting" ]; then
        break
    fi
    sleep 5
done

echo $image_id > slave_image

echo "start instance"
aliyun ecs StartInstances  --RegionId 'us-east-1' --InstanceId.1 $master_id
while true
do
	instances=`aliyun ecs DescribeInstances --RegionId us-east-1 --InstanceIds [\"$master_id\"]`
	status=`echo $instances | jq ".Instances.Instance[].Status" | tr -d '"'`
	echo "instances are $status ..."
	if [ "$status" == "Running" ]; then
		break
	fi
	sleep 3
done

while ! ssh -o StrictHostKeyChecking=no ubuntu@$master_ip "exit" 2>/dev/null; do
    echo "SSH is not available yet, retrying in 3 seconds..."
    sleep 3
done