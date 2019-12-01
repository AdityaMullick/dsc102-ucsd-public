#!/bin/bash

bid=''
files=''
verbose='false'
region='us-west-2'
label='emr-5.28.0'

print_usage() {
  printf "Usage: ..."
}

while getopts 'u:bf:vr:l:k:' flag; do
  case "${flag}" in
    u) user="${OPTARG}" ;;
    b) bid='true' ;;
    f) files="${OPTARG}" ;;
    v) verbose='true' ;;
    r) region="${OPTARG}" ;;
    l) label="${OPTARG}" ;;
    k) key="${OPTARG}" ;;
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
    instance_group_core='InstanceCount=4,InstanceType=m5.xlarge'
else
    instance_group_core='InstanceCount=4,InstanceType=m5.xlarge,BidPrice=OnDemandPrice'
fi

aws2 emr create-cluster \
--release-label "${label}" \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,"$instance_group_core" \
--use-default-roles \
--ec2-attributes KeyName="${key}" \
--applications Name=JupyterHub Name=Spark Name=Hadoop \
--name="$emr_cluster" \
--log-uri s3://"${s3_bucket}"

# --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://"${region}".elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://YOUR_BUCKET/YOUR_SHELL_SCRIPT.sh"]
