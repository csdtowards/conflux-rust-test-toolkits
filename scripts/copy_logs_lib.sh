function copy_file_from_slaves() {
  file_name=$1
  slave_ips=$2
  log_dir=$3
  dest_file_name=$4

  max_parallel=400
  job_count=0

  while IFS= read -r i; do
    scp -o "StrictHostKeyChecking no" "ubuntu@$i:~/$file_name" "$log_dir/logs_tmp/$i$dest_file_name" &

    job_count=$((job_count + 1))
    if [ "$job_count" -ge "$max_parallel" ]; then
      echo "wait"
      wait -n  # Wait for at least one background job to complete
      job_count=$((job_count - 1))
    fi

  done < "$slave_ips"

  wait
}

function copy_metrics_from_slaves() {
  file_name=$1
  slave_ips=$2
  log_dir=$3
  dest_file_name=$4

  max_parallel=400
  job_count=0

  while IFS= read -r i; do
    scp -o "StrictHostKeyChecking no" "ubuntu@$i:~/$file_name" "$log_dir/logs_metrics/$i$dest_file_name" &

    job_count=$((job_count + 1))
    if [ "$job_count" -ge "$max_parallel" ]; then
      wait -n  # Wait for at least one background job to complete
      job_count=$((job_count - 1))
    fi

  done < "$slave_ips"

  wait
}

function init_log_dir() {
  log_dir=$1

  rm -rf "$log_dir"
  mkdir -p "$log_dir/logs_tmp"
  mkdir -p "$log_dir/logs_metrics"
}

function wait_for_copy() {
  dest_file_name=$1
  while true
  do
      n=`ps -ef|grep [s]cp|grep "$dest_file_name"|grep -v grep|wc -l`
      if [ $n -eq 0 ]
      then
          break
      fi
      echo $n remaining to download log
      sleep 1
  done
}

function expand_logs() {
  log_dir=$1
  dest_file_name=$2

  max_parallel=1000
  job_count=0

  for file in `ls $log_dir/logs_tmp/*$dest_file_name`
  do
      tar_dir=${file%$dest_file_name}
      mkdir "$tar_dir"
      tar xzf "$file" -C "$tar_dir" &
      
      job_count=$((job_count + 1))

      if [ "$job_count" -ge "$max_parallel" ]; then
        echo "wait"
        wait -n
        job_count=$((job_count - 1))
      fi
  done

  wait

  for file in `ls $log_dir/logs_tmp/*$dest_file_name`
  do
    rm "$file"
  done

  for file in `ls $log_dir/logs_metrics/*$dest_file_name`
  do
      tar_dir=${file%$dest_file_name}
      mkdir "$tar_dir"
      tar xzf "$file" -C "$tar_dir" &
      
      job_count=$((job_count + 1))

      if [ "$job_count" -ge "$max_parallel" ]; then
        wait -n
        job_count=$((job_count - 1))
      fi
  done

  wait

  for file in `ls $log_dir/logs_metrics/*$dest_file_name`
  do
    rm "$file"
  done
}
