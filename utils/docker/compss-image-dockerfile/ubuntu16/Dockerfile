FROM ubuntu:16.04
MAINTAINER COMPSs Support <support-compss@bsc.es>

# =============================================================================
# Configuration required to use the image for jenkins testing
# =============================================================================

# Install Essentials
RUN apt-get update

# Install Packages
RUN apt-get install -y \
	git  \
        wget \
        openssh-server \
        sudo \
	openjdk-8-jdk

# gen dummy keys, centos doesn't autogen them like ubuntu does
RUN /usr/bin/ssh-keygen -A

# Set SSH Configuration to allow remote logins without /proc write access
RUN sed -ri 's/^session\s+required\s+pam_loginuid.so$/session optional pam_loginuid.so/' /etc/pam.d/sshd

# Create Jenkins User
RUN useradd jenkins -m -s /bin/bash

# Add the jenkins user to sudoers
RUN echo "jenkins  ALL=(ALL)  NOPASSWD:ALL" >> /etc/sudoers

# Set Name Servers
# COPY /files/resolv.conf /etc/resolv.conf

# Create ssh required dir
RUN mkdir /run/sshd

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

# =============================================================================
# Dependencies for building COMPSs
# =============================================================================

#RUN apt-get install -y \
        #ssh \
        #libcr-dev \
        #openssh-client \

RUN apt-get update

# Build dependencies
RUN apt-get -y install maven

# Runtime dependencies
RUN apt-get -y install openjdk-8-jdk graphviz xdg-utils

# Bindings-common-dependencies
RUN apt-get -y install libtool automake build-essential

# Python-binding dependencies
RUN apt-get -y install python-dev libpython2.7 python-pip python3-pip
RUN pip2 install numpy dill guppy decorator
RUN pip3 install numpy dill decorator # guppy not supported in python3

# Python-redis dependencies
RUN pip install redis redis-py-cluster
RUN pip3 install redis redis-py-cluster

# C-binding dependencies
RUN apt-get -y install libboost-all-dev libxml2-dev csh

# Extrae dependencies
RUN apt-get -y install libxml2 gfortran libpapi-dev papi-tools

# Misc. dependencies
RUN apt-get update
RUN apt-get -y install openmpi-bin openmpi-doc libopenmpi-dev uuid-runtime curl bc

RUN yes jenkins2017 | passwd

# Configure user environment

# =============================================================================
# System configuration
# =============================================================================

# Add environment variables
RUN echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment
RUN echo "MPI_HOME=/usr/lib64/openmpi" >> /etc/environment
RUN echo "LD_LIBRARY_PATH=/usr/lib64/openmpi/lib" >> /etc/environment

# Ensure that the jenkins user owns all his files
RUN chown -R jenkins /home/jenkins/
RUN chgrp -R jenkins /home/jenkins/

