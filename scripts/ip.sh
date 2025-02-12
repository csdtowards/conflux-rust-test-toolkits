#!/usr/bin/env bash
# sudo rm -rf /etc/hosts
export AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION:-us-west-2}

skip_ssh=0
if [[ $# -eq 0 ]]; then
    IP_NAME="PrivateIpAddress"
elif [[ $# -eq 1 ]] && [[ "$1" == "--public" ]]; then 
    IP_NAME="PublicIpAddress"
elif [[ $# -eq 2 ]] && [[ "$2" == "--ip" ]]; then 
    IP_NAME="PrivateIpAddress"
    skip_ssh=1
else
    echo "Invalid argument. Pass no argument to get private ips or --public for public ips."
fi

if [[ -f ips ]]; then
    mv ips ips_old
fi
touch ips
if [[ -f instances ]]
then
	instance=`cat instances`
	response=`aws ec2 describe-instances --instance-ids $instance`
	echo $response | jq ".Reservations[].Instances[].$IP_NAME" | tr -d '"' > ips_tmp
	uniq ips_tmp > ips
	rm ips_tmp
fi
echo GET `wc -l ips` IPs

if [ "$skip_ssh" -eq 1 ]; then
    line_count=$(wc -l < ips)
    if [ "$line_count" -gt 100 ]; then
        sam=$((line_count / 100))
        awk -v "r=$sam" 'NR % r == 1' ips > ips_metrics
    else
        cp ips ips_metrics
    fi

    if [ "$line_count" -gt 2000 ]; then
        awk 'NR % 3 == 1' ips > ips_sample
    else
        cp ips ips_sample
    fi
    exit 0
fi

ips=(`cat ips`)
for i in `seq 0 $((${#ips[@]}-1))`
do
#  echo ${ips[$i]} n$i |sudo tee -a /etc/hosts
  ssh -o "StrictHostKeyChecking no" ubuntu@${ips[$i]} "exit" &

  if (( (i + 1) % 2001 == 0 )); then
    sleep 1
  fi

done
wait
true
#scp ips_current lpl@blk:~/ips
#scp -i MyKeyPair.pem ips_current ubuntu@aws:~/ssd/ips
#ssh vm "./tmp.sh"
#rm ipof*
