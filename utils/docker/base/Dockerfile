#FROM ubuntu:20.04
FROM ubuntu:18.04
MAINTAINER COMPSs Support <support-compss@bsc.es>

# =============================================================================
# Configuration required to use the image for jenkins testing
# =============================================================================
ARG DEBIAN_FRONTEND=noninteractive
# Install Essentials
RUN apt-get update && \
# Install Packages
    apt-get install -y --no-install-recommends \
    git vim wget jq curl openssh-server sudo software-properties-common unzip && \ 
    add-apt-repository universe && \
# Enable repo compression
    git config --global core.compression 9 && \
# =============================================================================
# Dependencies for building COMPSs
# =============================================================================
# Build dependencies
    wget http://apache.rediris.es/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz && \
    tar -zxvf apache-maven-3.6.3-bin.tar.gz -C /opt/ && \
    sudo ln -s /opt/apache-maven-3.6.3/bin/mvn /bin/mvn && \
    rm apache-maven-3.6.3-bin.tar.gz && \
# Runtime dependencies
    apt-get install -y --no-install-recommends openjdk-8-jdk graphviz xdg-utils \
# Bindings-common-dependencies
    libtool automake build-essential \
# C-binding dependencies
    libboost-all-dev libxml2-dev \
# Extrae dependencies
    libxml2 gfortran libpapi-dev papi-tools \
# Misc. dependencies
    openmpi-bin openmpi-doc libopenmpi-dev uuid-runtime curl bc \
# Python-binding dependencies
    python python-setuptools python-dev libpython2.7 \ 
# python-pip
    python3 python3-setuptools python3-dev python3-pip \
# AutoParallel dependencies
    libgmp3-dev flex bison libbison-dev texinfo libffi-dev && \

# Python modules
    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \
    python2 get-pip.py && \
    rm get-pip.py && \
    python2 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade pip && \
    pip2 install --no-cache-dir wheel numpy==1.15.4 dill guppy decorator mpi4py==1.3.1 && \
    pip3 install --no-cache-dir wheel numpy==1.15.4 dill decorator mpi4py==3.0.1 && \
#Ubuntu:20    
#    pip2 install --no-cache-dir wheel numpy==1.15.4 dill guppy decorator mpi4py && \
#    pip3 install --no-cache-dir wheel numpy==1.15.4 dill decorator mpi4py && \

# Python-redis dependencies
    pip2 install --no-cache-dir redis==2.10.6 redis-py-cluster && \
    pip3 install --no-cache-dir redis==2.10.6 redis-py-cluster && \
# pycompsslib dependencies
    pip2 install --no-cache-dir scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 && \
    pip3 install --no-cache-dir scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 && \
#Ubuntu:20
#    pip2 install --no-cache-dir scipy scikit-learn pandas && \
#    pip3 install --no-cache-dir scipy scikit-learn pandas && \

# AutoParallel dependencies
    pip2 install --no-cache-dir astor sympy enum34 islpy && \

# Pycodestyle
    pip2 --no-cache-dir install install pycodestyle && \
    pip3 --no-cache-dir install install pycodestyle && \

# Streaming dependencies
    wget https://services.gradle.org/distributions/gradle-5.4.1-bin.zip && \
    unzip -d /opt gradle-5.4.1-bin.zip && \
    rm gradle-5.4.1-bin.zip && \
    ln -s /opt/gradle-5.4.1 /opt/gradle && \
    echo "GRADLE_HOME=/opt/gradle" >> /etc/environment && \
    pip2 --no-cache-dir install kafka-python && \
    pip3 --no-cache-dir install kafka-python && \
# Testing dependencies
    pip3 --no-cache-dir install enum34 tabulate && \
# Numba dependencies
    pip2 --no-cache-dir install llvmlite==0.31.0 numba && \
    pip3 --no-cache-dir install llvmlite>=0.31.0 numba && \
# Jupyter dependencies
    pip2 --no-cache-dir install jupyter ipykernel nbconvert==5.6.1 && \
    python2 -m ipykernel install && \
    pip3 --no-cache-dir install jupyter ipykernel nbconvert==5.6.1 && \
    python3 -m ipykernel install && \
# Coverage
    pip2 --no-cache-dir install coverage && \
    pip3 --no-cache-dir install coverage && \

# Create Jenkins User for CI/CD
    useradd jenkins -m -s /bin/bash && \
# Add the jenkins user to sudoers
    echo "jenkins  ALL=(ALL)  NOPASSWD:ALL" >> /etc/sudoers && \
# Enable ssh to localhost for user root & jenkins
    yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && \
    cp -r /root/.ssh /home/jenkins && \
# Make sure jenkins owns his files
    chown -R jenkins /home/jenkins/ && \
    chgrp -R jenkins /home/jenkins/ && \
# Configure user environment
# =============================================================================
# System configuration
# =============================================================================
# Add environment variables
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib/openmpi/lib" >> /etc/environment && \
    mkdir /run/sshd && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*
