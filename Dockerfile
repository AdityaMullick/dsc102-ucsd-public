# Use an official Python runtime as a parent image
FROM ubuntu:latest
WORKDIR /root
RUN apt-get update
RUN apt-get install -y curl jq unzip procps && rm -rf /var/lib/apt/lists/*
RUN curl "https://d1vvhvl2y92vvt.cloudfront.net/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
RUN unzip awscliv2.zip && ./aws/install && rm -rvf aws awscliv2.zip
COPY emr-launch emr-list emr-terminate /usr/local/bin/