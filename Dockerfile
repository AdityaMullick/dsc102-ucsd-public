FROM ubuntu:latest
WORKDIR /root
RUN apt-get update
RUN apt-get install -y curl jq unzip procps && rm -rf /var/lib/apt/lists/*
RUN curl "https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip && ./aws/install && rm -rvf aws awscliv2.zip
COPY emr-launch emr-list emr-terminate emr-dns s3-init /usr/local/bin/
ENV AWS_DEFAULT_REGION='us-west-2' \
    EMR_DEFAULT_LABEL='emr-5.28.0' \
    BOOTSTRAPE_PATH='s3://dsc102-pa2-public/bootstrap-scripts/setup-common.sh' \
    S3_LOGS_POSTFIX='-emr-logs' \
    S3_PA2_POSTFIX='-pa2' \
    EMR_CLUSTER_POSTFIX='-emr-cluster' \
    S3_PA2_SRC_FOLDER='src' \
    S3_PA2_PUBLIC='s3://dsc102-pa2-public'

