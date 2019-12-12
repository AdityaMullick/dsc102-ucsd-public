# Assignment 2 DSC 102 2020 WI

## Getting started

### Prerequisite
The only prerequisite of this assignment is docker, which can be found in https://docs.docker.com/install/. A container image ```yuhzhang/dsc102-pa2``` will be used as the client for all AWS-related operations.

Within the container you are provided with the following utilities

```bash
s3-init
emr-launch
emr-list
emr-dns
emr-terminate
```



### Prepare key pairs on AWS console
If you have not done so. Go to your [AWS console ](https://ets-apps.ucsd.edu/dsc102-custom-aws/ )and click key pairs. Follow the instructions to create or upload a public key to AWS. Note down the name of the key.

### 1. Store AWS credentials
Create a new text file named ```credentials.list``` and put the following content in it:
```bash
PID=...
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...
```
Fill in the blanks (placeholded by ```...```) with corresponding values. ```PID``` is your UCSD pid (e.g., a53230999) in lower case. You can find the rest three values from https://ets-apps.ucsd.edu/dsc102-custom-aws/?mode=env. Note this credential is only temporary. You will need to update this file if the token expires.

### 2. Initialize assignment-related S3 buckets
Use the following command to initialize the S3 buckets needed for this assignment:

```bash
docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 s3-init
```


This command will setup a EMR log bucket named ```<your pid>-emr-logs```, and a bucket storing your scripts named ```<your pid>-pa2```. It will also copy the assignment dev-kit to the latter bucket.

### 3. Launch an EMR cluster
1. Use emr-launch utility built in the docker image ```yuhzhang/dsc102-pa2```:
    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-launch -k <key name>
    ```
    
    The above utility will spawn a EMR cluster named ```<pid>-emr-cluster```. Available flags/arguments are:
    ```
    -k <key name>, the name of your secrets to access the cluster via ssh
    -b, optional, if set, your CORE instances will be run with Spot pricing
    -n <number of workers>, optional, the number of workers you want to have, default: 4 
    -d, optional, if set, use the deployment hardware m5.xlarge, otherwise use m4.large
    -t, optional, if set, setup Theia IDE on master node port 3000
    -f, optional, if set, force starting the second cluster, as by default only one cluster is permitted
    ```
1. Example usage

    Spawn a development cluster: 
    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-launch -k <key name> -n 2 -b -t
    ```
    Spawn a deployment cluster to test your scripts: 
    
    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-launch -k <key name> -n 4 -d -t
    ```
    This deployment environment is **fixed** and will be used to evaluate all of your submissions. 
1. Don't forget to terminate the cluster when you are done:

    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-terminate <cluster id>
    ```

### 4. Access your cluster

1. Use the following command to list the cluster IDs:
    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-list
    ```
    
1. Then query the DNS name of cluster's master node using:
    ```bash
    docker run --env-file path/to/credentials.list yuhzhang/dsc102-pa2 emr-dns <cluster ID>
    ```
    
    This will return the DNS name such as:
    
    ``````
    ec2-###-##-##-###.compute-1.amazonaws.com
    ``````
    
1. You can now SSH into your master node via:
    
    ```bash
    ssh -i path/to/key hadoop@ec2-###-##-##-###.compute-1.amazonaws.com
    ```
    
1. Follow the section *Set Up an SSH Tunnel to the Master Node Using Dynamic Port Forwarding* on [link](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-ssh-tunnel.html).

1. Follow instruction on [link](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html) to setup the proxy to your master node.

1. In your browser, access
    ```
    ec2-###-##-##-###.compute-1.amazonaws.com:8888
    ```
    this will direct you to the jupyter notebook running on the master node. The password would be you pid.
    
1. Optionally and if you used ```-t``` flag when launching the cluster, you can access Theia IDE running on 
    ```
    ec2-###-##-##-###.compute-1.amazonaws.com:3000
    ```
    
1. In Jupyter notebook, rename ```assignment2.ipynb``` to ```<your pid>_assignment2.ipynb``` and continue the assignment by following the instructions written in the notebook.