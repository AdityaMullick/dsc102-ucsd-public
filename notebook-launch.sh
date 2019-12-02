#!/bin/bash



print_usage() {
  printf "Usage: ..."
}

while getopts 'i:' flag; do
  case "${flag}" in
    i) cluster_id="${OPTARG}" ;;
    b) bid='true' ;;
    f) files="${OPTARG}" ;;
    v) verbose='true' ;;
    r) region="${OPTARG}" ;;
    l) label="${OPTARG}" ;;
    k) key="${OPTARG}" ;;
    n) num_worker=${OPTARG} ;;
    *) print_usage
       exit 1 ;;
  esac
done

s3_bucket=${user}-emr-logs
emr_cluster=${user}-emr-cluster
if [[ -n "$user" ]]; then
    if aws2 s3api head-bucket --bucket "$s3_bucket" 2>/dev/null; then
        echo "emr logs bucket already exists, skipping"
    else
        echo "creating emr logs bucket"
        aws2 s3api create-bucket --acl private --bucket "${s3_bucket}" --region "${region}" --create-bucket-configuration LocationConstraint="${region}"
    fi
else
    echo "argument error"
fi

if [ -n "$bid" ]; then
    instance_group_core="InstanceCount=$num_worker,InstanceType=m5.xlarge"
else
    instance_group_core="InstanceCount=$num_worker,InstanceType=m5.xlarge,BidPrice=OnDemandPrice"
fi

ret=$(aws2 emr add-steps --cluster-id "${cluster_id}" --steps 'Name=LoadData,Jar=command-runner.jar,ActionOnFailure=CONTINUE,Type=CUSTOM_JAR,Args=s3-dist-cp,--src,s3://my-tables/incoming/hourly_table,--dest,/data/input/hourly_table,--targetSize,10,--groupBy,.*/hourly_table/.*(2017-).*/(\d\d)/.*\.(log)'

)
echo $ret
# --bootstrap-actions Path="s3://dsc102-scripts/setup.sh" \
echo "Cluster starting ..."
# --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://"${region}".elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://YOUR_BUCKET/YOUR_SHELL_SCRIPT.sh"]
