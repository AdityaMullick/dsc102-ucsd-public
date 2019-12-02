#!/bin/bash
set -e
cleanup() {
    if [ -n "$cluster_id" ]; then 
        echo "Terminating ... cluster ID: ${cluster_id}"; 
        aws2 emr terminate-clusters --cluster-ids "$cluster_id"
        sleep 5
    fi
    exit
}

update_script_bucket(){
    aws2 s3api put-bucket-versioning --bucket "${s3_bucket_scripts}" --versioning-configuration Status=Enabled &&
    aws2 s3 sync "s3://dsc102-pa2" "s3://${s3_bucket_scripts_folder}"
}

cluster_id=''
trap cleanup INT

bid=''
region='us-west-2'
label='emr-5.28.0'
num_worker=4

print_usage() {
  printf "Not written yet"
}

while getopts 'u:bf:vr:l:k:n:' flag; do
  case "${flag}" in
    u) user="${OPTARG}" ;;
    b) bid='true' ;;
    r) region="${OPTARG}" ;;
    l) label="${OPTARG}" ;;
    k) key="${OPTARG}" ;;
    n) num_worker=${OPTARG} ;;
    *) print_usage
       exit 1 ;;
  esac
done

s3_bucket_logs=${user}-emr-logs
s3_bucket_scripts=${user}-pa2
s3_bucket_scripts_folder=${s3_bucket_scripts}/src
emr_cluster=${user}-emr-cluster
if [[ -n "$user" ]]; then
    if aws2 s3api head-bucket --bucket "$s3_bucket_logs" 2>/dev/null; then
        echo "emr logs bucket already exists, skipping"
    else
        echo "creating emr logs bucket"
        aws2 s3api create-bucket --acl private --bucket "${s3_bucket_logs}" --region "${region}" --create-bucket-configuration LocationConstraint="${region}"
    fi
    if aws2 s3api head-bucket --bucket "$s3_bucket_scripts" 2>/dev/null; then
        echo "emr scripts bucket already exists, updating the bucket with missing files"
        update_script_bucket
    else
        echo "creating emr scripts bucket"
        aws2 s3api create-bucket --acl private --bucket "${s3_bucket_scripts}" --region "${region}" --create-bucket-configuration LocationConstraint="${region}" &&
        until [ aws2 s3api head-bucket --bucket "$s3_bucket_scripts" 2>/dev/null ]; do
            echo "waiting for bucket setting up"
            sleep 20
        done
        echo "copying pa2 scripts ..."
        update_script_bucket

    fi
else
    echo "argument error"
fi


if [ -n "$bid" ]; then
    instance_group_core="InstanceCount=$num_worker,InstanceType=m5.xlarge"
else
    instance_group_core="InstanceCount=$num_worker,InstanceType=m5.xlarge,BidPrice=OnDemandPrice"
fi

ret=$(aws2 emr create-cluster \
--release-label "${label}" \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge InstanceGroupType=CORE,"$instance_group_core" \
--use-default-roles \
--ec2-attributes KeyName="${key}",SubnetId="subnet-5ea74226"  \
--applications Name=JupyterHub Name=Spark Name=Hadoop Name=Livy Name=Hive \
--name="$emr_cluster" \
--log-uri s3://"${s3_bucket_logs}" \
--bootstrap-actions Path="s3://dsc102-scripts/setup-common.sh",Args=["${user}","s3://${s3_bucket_scripts_folder}"] \
--configurations '[{"Classification":"spark","Properties":{"maximizeResourceAllocation":"true","spark.dynamicAllocation.enabled":"false"}}]'
) &&
cluster_id=$(echo $ret | jq -r '.ClusterId')

if [  -z "$cluster_id" ]; then
    echo "cluster failed to start"
    exit
fi
# 
echo "Cluster starting ... cluster ID: ${cluster_id}"
sleep 20
state=''
until [ "$state" = "WAITING" ]; do
    state=$(aws2 emr describe-cluster --cluster-id "${cluster_id}" | jq -r .Cluster.Status.State)
    echo "Current cluster state: ${state}"
    if [[ ("${state}" = "TERMINATED") || ("${state}" = "TERMINATING") || ("${state}" = "TERMINATED_WITH_ERRORS") ]]; then
        echo "Cluster terminated accidently. Check logs on AWS console."
        break
    fi
    sleep 20
done


# --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://"${region}".elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://YOUR_BUCKET/YOUR_SHELL_SCRIPT.sh"]
