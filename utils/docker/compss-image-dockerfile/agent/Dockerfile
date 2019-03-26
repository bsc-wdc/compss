FROM ubuntu:16.04

RUN apt-get update && \
    apt-get install -y \
        csh \
        openjdk-8-jdk  \
        openssh-client \
        openssh-server \
        ssh \
        vim && \
    mkdir /var/run/sshd && \
    echo 'root:screencast' | chpasswd && \
    sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed 's@session\s*required\s*pam_loginuid.so@session optional pam_loginuid.so@g' -i /etc/pam.d/sshd && \
    echo "export VISIBLE=now" >> /etc/profile && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* && \
    yes yes | ssh-keygen -f /root/.ssh/id_rsa -t rsa -N '' > /dev/null && \
    cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys

COPY ["content/", "/"]
ENV AGENT_HOST=localhost AGENT_PORT=46001 DEBUG=off DATACLAY_ARG="" APP_JAR=/app/app.jar DATACLAY_EP=127.0.0.1:11034
WORKDIR /tmp

CMD /opt/COMPSs/Runtime/scripts/system/adaptors/RESTagent/startAgent.sh -h ${AGENT_HOST} -p ${AGENT_PORT} -d ${DEBUG} -a ${APP_JAR} ${DATACLAY_ARG} -lm ${DATACLAY_EP}

