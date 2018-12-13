FROM compss/base:latest
MAINTAINER COMPSs Support <support-compss@bsc.es>

# Configure user environment
# =============================================================================
# System configuration
# =============================================================================
# Add environment variables
RUN echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib64/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib64/openmpi/lib" >> /etc/environment && \
    mkdir /run/sshd

# Copy framework files for installation and testing
COPY . /framework

# Install COMPSs
RUN cd /framework && \
    ./submodules_get.sh && \
    ./submodules_patch.sh && \
    sudo -E /framework/builders/buildlocal /opt/COMPSs && \
    cp /framework/utils/docker/entrypoint.sh / && \
    rm -r /framework

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$JAVA_HOME/jre/lib/amd64/server

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

# If container is run in interactive, entrypoint runs the sshd server
ENTRYPOINT ["/entrypoint.sh"]
