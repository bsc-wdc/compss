FROM ubuntu:16.04
MAINTAINER COMPSs Support <support-compss@bsc.es>

# =============================================================================
# Configuration required to use the image for jenkins testing
# =============================================================================

# Install Essentials
RUN apt-get update && \
# Install Packages
    apt-get install -y --no-install-recommends \
	git  \
    vim \
    wget \
    openssh-server \
    sudo && \
# Create Jenkins User
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
# Enable repo compression
    git config --global core.compression 9 && \
# =============================================================================
# Dependencies for building COMPSs
# =============================================================================
# Build dependencies
    apt-get install -y --no-install-recommends maven \
# Runtime dependencies
    openjdk-8-jdk graphviz xdg-utils \
# Bindings-common-dependencies
    libtool automake build-essential \
# C-binding dependencies
    libboost-all-dev libxml2-dev csh \
# Extrae dependencies
    libxml2 gfortran libpapi-dev papi-tools \
# Misc. dependencies
    openmpi-bin openmpi-doc libopenmpi-dev uuid-runtime curl bc \
# Python-binding dependencies
    python-dev python3-dev libpython2.7 python-pip python3-pip python-setuptools python3-setuptools && \
    pip2 install wheel numpy==1.15.4 dill guppy decorator mpi4py==1.3.1 && \
    pip3 install wheel numpy==1.15.4 dill decorator mpi4py==3.0.1 && \
# Python-redis dependencies
    pip2 install redis==2.10.6 redis-py-cluster && \
    pip3 install redis==2.10.6 redis-py-cluster && \
# pycompsslib dependencies
    pip2 install scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 && \
    pip3 install scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 && \
# AutoParallel dependencies
    apt-get install -y --no-install-recommends libgmp3-dev flex bison libbison-dev texinfo libffi-dev && \
    pip2 install astor sympy enum34 islpy && \
# Testing dependencies
    pip3 install enum34 tabulate && \
# Configure user environment
# =============================================================================
# System configuration
# =============================================================================
# Add environment variables
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib/openmpi/lib" >> /etc/environment && \
    mkdir /run/sshd && \
    rm -rf /var/lib/apt/lists/*
