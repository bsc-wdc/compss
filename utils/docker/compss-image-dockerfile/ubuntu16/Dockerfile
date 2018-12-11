FROM ubuntu:16.04
MAINTAINER COMPSs Support <support-compss@bsc.es>

# =============================================================================
# Configuration required to use the image for jenkins testing
# =============================================================================

# Install Essentials
RUN apt-get update && \
# Install Packages
    apt-get install -y \
	git  \
    vim \
    wget \
    openssh-server \
    sudo && \
# gen dummy keys, centos doesn't autogen them like ubuntu does
    /usr/bin/ssh-keygen -A && \
# Set SSH Configuration to allow remote logins without /proc write access
    sed -ri 's/^session\s+required\s+pam_loginuid.so$/session optional pam_loginuid.so/' /etc/pam.d/sshd && \
# Create Jenkins User
    useradd jenkins -m -s /bin/bash && \
# Add the jenkins user to sudoers
    echo "jenkins  ALL=(ALL)  NOPASSWD:ALL" >> /etc/sudoers && \
# Create ssh required dir
    mkdir /run/sshd && \
# Enable repo compression
    git config --global core.compression 9

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

# =============================================================================
# Dependencies for building COMPSs
# =============================================================================

RUN apt-get update && \
# Build dependencies
    apt-get -y install maven && \
# Runtime dependencies
    apt-get -y install openjdk-8-jdk graphviz xdg-utils && \
# Bindings-common-dependencies
    apt-get -y install libtool automake build-essential && \
# Python-binding dependencies
    apt-get -y install python-dev libpython2.7 python-pip python3-pip && \
    pip2 install numpy dill guppy decorator && \
    pip3 install numpy dill decorator  && \
# Python-redis dependencies
    pip2 install redis redis-py-cluster && \
    pip3 install redis redis-py-cluster && \
# pycompsslib dependencies
    pip2 install numpy==1.15.4 scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 matplotlib==2.2.3 flake8 psutil && \
    pip3 install numpy==1.15.4 scipy==1.0.0 scikit-learn==0.19.1 pandas==0.23.1 matplotlib==2.2.3 flake8 codecov coverage psutil && \
# C-binding dependencies
    apt-get -y install libboost-all-dev libxml2-dev csh && \
# Extrae dependencies
    apt-get -y install libxml2 gfortran libpapi-dev papi-tools && \
# Misc. dependencies
    apt-get update && \
    apt-get -y install openmpi-bin openmpi-doc libopenmpi-dev uuid-runtime curl bc && \
    yes jenkins2017 | passwd && \
# AutoParallel dependencies
    apt-get -y install libgmp3-dev flex bison libbison-dev texinfo libffi-dev && \
    pip2 install astor sympy enum34 islpy && \
# Configure user environment
# =============================================================================
# System configuration
# =============================================================================
# Add environment variables
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib64/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib64/openmpi/lib" >> /etc/environment && \
# Ensure that the jenkins user owns all his files
    chown -R jenkins /home/jenkins/ && \
    chgrp -R jenkins /home/jenkins/ && \
    apt-get clean && \
    sudo rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


