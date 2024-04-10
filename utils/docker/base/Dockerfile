FROM ubuntu:20.04 as base
LABEL maintainer="COMPSs Support <support-compss@bsc.es>" \
      vendor="Barcelona Supercomputing Center (BSC)" \
      url="http://compss.bsc.es"

ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH=amd

FROM base as base_amd64

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/amd64/server:$LD_LIRBRARY_PATH
ENV EXTRAE_MPI_HEADERS /usr/include/x86_64-linux-gnu/mpi

FROM base as base_arm64

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-arm64/
ENV LD_LIBRARY_PATH=$JAVA_HOME/jre/lib/aarch64/server:$LD_LIRBRARY_PATH
ENV EXTRAE_MPI_HEADERS /usr/include/aarch64-linux-gnu/mpi


FROM base_$TARGETARCH as base_rt

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jdk graphviz xdg-utils uuid-runtime && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*

FROM base_rt as base_python

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3 && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*

FROM base_python as base_all

RUN apt-get update && \
    apt-get install -y --no-install-recommends libboost-all-dev libxml2 papi-tools openmpi-bin libopenmpi-dev libtool automake build-essential python3-pip openssh-server jq bc && \
    pip3 install --no-cache-dir wheel dill guppy3==3.1.0 decorator mpi4py kafka-python && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir /run/sshd && \
    yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

FROM base_all as base_tutorial

RUN apt-get update && \
    apt-get install -y --no-install-recommends git vim wget ca-certificates maven curl unzip cmake libopenblas-dev && \
    pip3 install --no-cache-dir graphviz matplotlib>=2.2.3 numba==0.56.4 numpy==1.23.5 numpydoc>=0.8.0 scipy>=1.3.0 cvxpy>=1.1.5 scikit-learn>=1.0.2 python-dateutil==2.8.2 pandas>=0.24.2 tabulate==0.8.9 jupyter ipykernel ipython==8.3.0 jupyterlab && \
    python3 -m ipykernel install && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/*

FROM base_tutorial as base_ci

RUN apt-get update && \

    #Required for compilation and tests
    apt-get install -y --no-install-recommends libxml2-dev libpapi-dev python3-setuptools python3-dev gfortran \
    sudo firefox gnupg lsb-release libgraphviz-dev julia && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 --no-cache-dir install pycodestyle selenium redis==3.5.3 redis-py-cluster==2.1.3 enum34 tabulate black==24.4.0 memory-profiler && \
    pip3 --no-cache-dir install pytest pytest-cov nbval nbconvert mistune==2.0.5 coverage pytest-html-profiling pytest-metadata pytest-profiling pytest-subprocess pytest-sugar pydocstyle && \
    pip3 --no-cache-dir install black[jupyter]==24.4.0 && \
    # for fixing a nbcovert jinja2 compatibility problem
    pip3 --no-cache-dir install jinja2==3.0.3 && \
    pip3 --no-cache-dir install mypy==0.990 mypy-extensions==0.4.3 py types-tabulate types-decorator && \
    wget https://services.gradle.org/distributions/gradle-5.4.1-bin.zip && \
    unzip -d /opt gradle-5.4.1-bin.zip && \
    rm gradle-5.4.1-bin.zip && \
    mv /opt/gradle-5.4.1 /opt/gradle && \
    # Docker-CLI
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null && \
   apt-get update && \
   apt-get install -y docker-ce-cli && \
   apt-get autoclean && \
   rm -rf /var/lib/apt/lists/* && \
   # Geckodriver
   #curl -s https://api.github.com/repos/mozilla/geckodriver/releases/latest | grep "browser_download_url.*linux64.tar.gz" | cut -d : -f 2,3 | tr -d '\"' | head -n 1 | wget -i - && \
   wget https://github.com/mozilla/geckodriver/releases/download/v0.32.2/geckodriver-v0.32.2-linux64.tar.gz && \
   tar -xvf *linux64.tar.gz && \
   rm *linux64.tar.gz && \
   mv geckodriver /usr/local/bin/ && \
   # Create Jenkins User
   useradd jenkins -m -s /bin/bash && \
   # Add the jenkins user to sudoers
   echo "jenkins  ALL=(ALL)  NOPASSWD:ALL" >> /etc/sudoers && \
   # Enable ssh to localhost for user jenkins
   cp -r /root/.ssh /home/jenkins && \
   # Make sure jenkins owns his files
   chown -R jenkins /home/jenkins/ && chgrp -R jenkins /home/jenkins/ && \
   groupadd docker && \
   usermod -aG docker jenkins && \
   newgrp docker

