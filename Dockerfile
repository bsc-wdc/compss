FROM compss/base:latest
MAINTAINER COMPSs Support <support-compss@bsc.es>

ARG release=false

# Copy framework files for installation and testing
COPY . /framework

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$JAVA_HOME/jre/lib/amd64/server

# Install COMPSs
RUN cd /framework && \
    ./submodules_get.sh && \
    ./submodules_patch.sh && \
    sudo -E /framework/builders/buildlocal /opt/COMPSs && \
    mv /root/.m2 /home/jenkins/ && \
    rm -rf /root/.cache && \
    sudo chown -R jenkins: /framework && \
    sudo chown -R jenkins: /home/jenkins/ && \
    if [ "$release" = "true" ]; then rm -rf /framework /home/jenkins/.m2 /root/.m2; fi

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]
