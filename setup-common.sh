#!/bin/bash
set -e
JUPYTER_PASSWORD=${1:-"root"}
NOTEBOOK_DIR=${2:-"s3://YourBucketForNotebookCheckpoints/yourNotebooksFolder/"}
REGION=${3:-"us-west-2"}
# ---------------------------- common ------------------------------------

# mount home to /mnt
if [ ! -d /mnt/home ]; then
  	  sudo mv /home/ /mnt/
  	  sudo ln -s /mnt/home /home
fi
sudo yum update -y
sudo yum install -y htop git ;
sudo pip-3.6 install paramiko nltk scipy scikit-learn pandas matplotlib gensim pyspark==2.4.4 jupyter jupyter_contrib_nbextensions jupyter_nbextensions_configurator;
sudo /usr/local/bin/jupyter contrib nbextension install --system ;
sudo /usr/local/bin/jupyter nbextensions_configurator enable --system ;
sudo /usr/local/bin/jupyter nbextension enable code_prettify/code_prettify;
sudo /usr/local/bin/jupyter nbextension enable execute_time/ExecuteTime;
sudo /usr/local/bin/jupyter nbextension enable collapsible_headings/main;

echo 'export PYSPARK_PYTHON="/usr/bin/python3.6"' >> $HOME/.bashrc && source $HOME/.bashrc
echo 'export SPARK_HOME="/usr/lib/spark"' >> $HOME/.bashrc && source $HOME/.bashrc
# -----------------------------------------------------------------------------

# ---------------------------- master only ------------------------------------
is_master='false'
is_master=$(jq -r '.isMaster' "/mnt/var/lib/info/instance.json")

if [ "$is_master" = "true" ]; then 
	sudo yum install automake fuse fuse-devel gcc-c++ libcurl-devel libxml2-devel make openssl-devel -y

    # extract BUCKET and FOLDER to mount from NOTEBOOK_DIR
	NOTEBOOK_DIR="${NOTEBOOK_DIR%/}/"
	BUCKET=$(python3.6 -c "print('$NOTEBOOK_DIR'.split('//')[1].split('/')[0])")
	FOLDER=$(python3.6 -c "print('/'.join('$NOTEBOOK_DIR'.split('//')[1].split('/')[1:-1]))")

    # install s3fs
    cd /mnt
    git clone https://github.com/s3fs-fuse/s3fs-fuse.git
    cd s3fs-fuse/
    # Master version does not work
    git checkout tags/v1.85 
    ./autogen.sh
    ./configure
    make
    sudo make install
    sudo su -c 'echo user_allow_other >> /etc/fuse.conf'
    mkdir -p /mnt/s3fs-cache
    mkdir -p /mnt/$BUCKET
    /usr/local/bin/s3fs -o iam_role=auto \
        -o umask=0 \
        -o dbglevel=info \
        -o curldbg \
        -o allow_other \
        -o use_cache=/mnt/s3fs-cache \
        -o no_check_certificate \
        -o enable_noobj_cache \
        -o url="https://s3-${REGION}.amazonaws.com" \
        $BUCKET \
        /mnt/$BUCKET
    # jupyter configs
    mkdir -p ~/.jupyter
    touch ls ~/.jupyter/jupyter_notebook_config.py
    HASHED_PASSWORD=$(python3.6 -c "from notebook.auth import passwd; print(passwd('$JUPYTER_PASSWORD'))")
    echo "c.NotebookApp.password = u'$HASHED_PASSWORD'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.open_browser = False" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.ip = '*'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.NotebookApp.notebook_dir = '/mnt/$BUCKET/$FOLDER'" >> ~/.jupyter/jupyter_notebook_config.py
    echo "c.ContentsManager.checkpoints_kwargs = {'root_dir': '.checkpoints'}" >> ~/.jupyter/jupyter_notebook_config.py
  
    ### Setup Jupyter deamon and launch it
    cd ~
    echo "Creating Jupyter Daemon"
  
    sudo cat <<EOF > /home/hadoop/jupyter.conf
description "Jupyter"

start on runlevel [2345]
stop on runlevel [016]

respawn
respawn limit 0 10

chdir /mnt/$BUCKET/$FOLDER

script
sudo su - hadoop > /var/log/jupyter.log 2>&1 <<BASH_SCRIPT
      export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
      export PYSPARK_PYTHON=/usr/bin/python3.6
      export JAVA_HOME="/etc/alternatives/jre"
      jupyter notebook --log-level=INFO
BASH_SCRIPT

end script
EOF
  
      sudo mv /home/hadoop/jupyter.conf /etc/init/
      sudo chown root:root /etc/init/jupyter.conf
      sudo initctl reload-configuration
      # start jupyter daemon
      echo "Starting Jupyter Daemon"
      sudo initctl start jupyter

fi

# -----------------------------------------------------------------------------






