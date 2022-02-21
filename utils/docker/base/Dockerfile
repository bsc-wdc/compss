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
    git vim wget curl openssh-server sudo software-properties-common unzip && \
    add-apt-repository universe && \
    apt-get install -y jq && \
# Enable repo compression
    git config --global core.compression 9 && \
# Firefox
    apt install -y firefox && \
# Geckodriver
    curl -s https://api.github.com/repos/mozilla/geckodriver/releases/latest | grep "browser_download_url.*linux64.tar.gz" | cut -d : -f 2,3 | tr -d '\"' | head -1 | wget -i - && \
    tar -xvf *linux64.tar.gz && \
    rm *linux64.tar.gz && \
    mv geckodriver /usr/local/bin/ && \
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
#    curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py && \  WARNING: python 2 support has been removed.
    curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py && \
    python2 get-pip.py && \
    rm get-pip.py && \
    python2 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade pip && \
    pip2 install --no-cache-dir wheel numpy dill guppy==0.1.11 decorator mpi4py==1.3.1 && \
    pip3 install --no-cache-dir wheel numpy dill guppy3==3.1.0 decorator mpi4py==3.0.1 selenium && \
#Ubuntu:20
#    pip2 install --no-cache-dir wheel numpy==1.15.4 dill guppy==0.1.11 decorator mpi4py && \
#    pip3 install --no-cache-dir wheel numpy==1.15.4 dill guppy3==3.1.0 decorator mpi4py && \
# mpmath dependencies: TODO: To be removed if python2 support is dropped
    pip2 install --no-cache-dir setuptools-scm==5.0.2 && \
    pip3 install --no-cache-dir setuptools-scm==5.0.2 && \
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
# Mypy
    # pip3 --no-cache-dir install install mypy mypy-extensions types-tabulate types-decorator && \
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
# Testing
    pip2 --no-cache-dir install nose matplotlib && \
    pip3 --no-cache-dir install nose matplotlib && \
    pip3 --no-cache-dir install pytest pytest-cov pytest-notebook nbval && \
    # Patch guppy for python2
    file_to_patch=$(python2 -c "from __future__ import print_function; import guppy.heapy.View as hpv; print(hpv.__file__[:-1]) if hpv.__file__.endswith('c') else print(hpv.__file__)") && \
    sed -i -E 's/if mod is None:/if mod is None or getattr(mod, "__dict__", None) is None:/g' $file_to_patch  && \
# FIXES: Rollback to latest snapshot running versions. Updates numpy for numba.
    pip3 install charset-normalizer==2.0.11 --upgrade  && \
    pip3 install jupyter-core==4.9.1 --upgrade && \
    pip3 uninstall pathlib2 && \
    pip3 install prompt-toolkit==3.0.26 --upgrade && \
    pip3 install pytest==7.0.0 --upgrade && \
    pip3 install typing_extensions==4.0.1 --upgrade && \
    pip3 install jupyter-client==6.1.12 --upgrade && \
    pip3 install pytest-asyncio==0.16.0 --upgrade && \
    pip2 install pathlib2==2.3.6 --upgrade && \
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
# Docker-CLI
    apt-get install -y ca-certificates curl gnupg lsb-release && \
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
        $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
    apt-get update && \
    apt-get install -y docker-ce-cli && \
    groupadd docker && \
    usermod -aG docker jenkins && \
    newgrp docker && \
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
