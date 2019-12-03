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
    n=0
    retrytime=3
    until aws2 s3api head-bucket --bucket "$s3_bucket_scripts" 2>/dev/null; do
        if [ $n -ge $retrytime ]; then
            break
        fi
        ((++n))
        echo "Bucket not up, retrying ..."
        sleep 20
    done

    if [ $n -le $retrytime ]; then
        aws2 s3api put-bucket-versioning --bucket "${s3_bucket_scripts}" --versioning-configuration Status=Enabled
        aws2 s3 sync "s3://dsc102-pa2" "s3://${s3_bucket_scripts_folder}"
    else
        echo "Bucket not up, retry timeout"
        exit 1
    fi
}

cluster_id=''
trap cleanup INT

spot=''
region='us-west-2'
label='emr-5.28.0'
num_worker=4

print_usage() {
  printf "Not written yet"
}

while getopts 'u:bf:vr:l:k:n:d:' flag; do
  case "${flag}" in
    u) pid="${OPTARG}" ;;
    b) spot='true' ;;
    r) region="${OPTARG}" ;;
    l) label="${OPTARG}" ;;
    k) key="${OPTARG}" ;;
    n) num_worker=${OPTARG} ;;
    d) dev="${OPTARG}" ;;
    *) print_usage
       exit 1 ;;
  esac
done

s3_bucket_logs=${pid}-emr-logs
s3_bucket_scripts=${pid}-pa2
s3_bucket_scripts_folder=${s3_bucket_scripts}/src
emr_cluster=${pid}-emr-cluster
if [[ -n "$pid" ]]; then
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
        echo "copying pa2 scripts ..."
        update_script_bucket

    fi
else
    echo "argument error"
fi

if [ "$dev" = "dev" ]; then
    instance_type="m4.large"
    spot='true'
else
    instance_type="m5.xlarge"
fi

if [ -n "$spot" ]; then
    instance_group_core="InstanceCount=$num_worker,InstanceType=$instance_type"
else
    instance_group_core="InstanceCount=$num_worker,InstanceType=$instance_type,BidPrice=OnDemandPrice"
fi

ret=$(aws2 emr create-cluster \
--release-label "${label}" \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$instance_type InstanceGroupType=CORE,"$instance_group_core" \
--use-default-roles \
--ec2-attributes KeyName="${key}",SubnetId="subnet-5ea74226"  \
--applications Name=Spark Name=Hadoop \
--name="$emr_cluster" \
--log-uri s3://"${s3_bucket_logs}" \
--bootstrap-actions Path="s3://dsc102-scripts/setup-common.sh",Args=["${pid}","s3://${s3_bucket_scripts_folder}","${region}"] \
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

if [ "$state" = "WAITING" ]; then
    echo "Cluster is up"
fi


# --steps Type=CUSTOM_JAR,Name=CustomJAR,ActionOnFailure=CONTINUE,Jar=s3://"${region}".elasticmapreduce/libs/script-runner/script-runner.jar,Args=["s3://YOUR_BUCKET/YOUR_SHELL_SCRIPT.sh"]
