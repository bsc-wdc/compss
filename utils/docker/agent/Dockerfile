# =============================================================================
# Image for building COMPSs
# =============================================================================
FROM compss/base18:latest as builder
MAINTAINER COMPSs Support <support-compss@bsc.es>

ENV GRADLE_HOME=/opt/gradle JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/opt/gradle/bin CLASSPATH=$CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$JAVA_HOME/jre/lib/amd64/server COMPSS_HOME=/opt/COMPSs/

COPY . /framework

RUN cd /framework && \
    ./submodules_get.sh && \
    ./submodules_patch.sh && \
    sudo -E /framework/builders/buildlocal -A -M -T -K --skip-tests /opt/COMPSs 

# =============================================================================
# Image for running COMPSs agent
# =============================================================================
FROM ubuntu:18.04 as base
MAINTAINER COMPSs Support <support-compss@bsc.es>

RUN apt-get update && \
# Install Packages
    apt-get install -y --no-install-recommends openjdk-8-jdk \
# Dependencies
    curl build-essential openmpi-bin openmpi-doc libopenmpi-dev\
# Python-binding dependencies
    python python-setuptools python-dev libpython2.7 \
# python-pip
    python3 python3-setuptools python3-dev python3-pip &&\
# Python modules
    curl https://bootstrap.pypa.io/pip/2.7/get-pip.py -o get-pip.py && \
    python2 get-pip.py && \
    rm get-pip.py && \
    python2 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade pip && \
    pip2 install --no-cache-dir wheel numpy==1.15.4 dill guppy==0.1.11 decorator mpi4py==1.3.1 && \
    pip3 install --no-cache-dir wheel numpy==1.15.4 dill guppy3==3.1.0 decorator mpi4py==3.0.1 && \
# Add environment variables
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib/openmpi/lib" >> /etc/environment && \
    mkdir /run/sshd && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*

# SET UP ENVIRONMENT
ENV AGENT_HOST=localhost REST_AGENT_PORT=0 COMM_AGENT_PORT=0 DEBUG=debug APP_PATH=/app JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
WORKDIR /tmp

# COPY COMPSs INSTALLATION
COPY --from=builder /opt/COMPSs /opt/COMPSs

CMD /opt/COMPSs/Runtime/scripts/user/compss_agent_start --hostname=${AGENT_HOST} --comm_port="${COMM_AGENT_PORT}" --rest_port="${REST_AGENT_PORT}" -d ${DEBUG} --classpath=${APP_PATH} --pythonpath=${APP_PATH} --log_dir=/tmp

