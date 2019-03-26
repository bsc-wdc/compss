# =============================================================================
# Base image for building and running COMPSs
# =============================================================================
FROM ubuntu:16.04 as base
# INSTALL DEPENDENCIES & ENVIRONMENT DEFINITION
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-8-jdk && \
    echo "JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment && \
    echo "MPI_HOME=/usr/lib/openmpi" >> /etc/environment && \
    echo "LD_LIBRARY_PATH=/usr/lib/openmpi/lib" >> /etc/environment && \
    mkdir -p /opt/COMPSs

# =============================================================================
# Image for building COMPSs
# =============================================================================
FROM base as builder

RUN apt-get install -y --no-install-recommends \
# Build dependencies
     maven python  &&\
# Configure user environment
    mkdir /run/sshd && \
    rm -rf /var/lib/apt/lists/*

# Copy framework files for installation
COPY . /framework

# Install COMPSs framework on the builder container
RUN /framework/builders/buildlocal -N --skip-tests && \
# Remove unnecessary adaptors
    rm -rf  \ 
    /opt/COMPSs/Runtime/adaptors/gat \
    /opt/COMPSs/Runtime/adaptors/nio \
    /opt/COMPSs/Runtime/adaptors/ws \
# Remove unnecessary schedulers
    /opt/COMPSs/Runtime/scheduler/compss-scheduler-fullGraph-multiobjective.jar \
    /opt/COMPSs/Runtime/scheduler/compss-scheduler-fifo.jar \
    /opt/COMPSs/Runtime/scheduler/compss-scheduler-lifo.jar \
# Remove unnecessary cloud connectors    
    /opt/COMPSs/Runtime/connectors/*


# =============================================================================
# Image for running COMPSs agent
# =============================================================================
FROM base

# SET UP ENVIRONMENT
ENV AGENT_HOST=localhost AGENT_PORT=46100 DEBUG=off DATACLAY_ARG="-DC " APP_JAR=/app/app.jar DATACLAY_EP=127.0.0.1:1034
WORKDIR /tmp

# COPY COMPSs INSTALLATION
COPY --from=builder /opt/COMPSs /opt/COMPSs

CMD /opt/COMPSs/Runtime/scripts/system/adaptors/RESTagent/startAgent.sh -h ${AGENT_HOST} -p ${AGENT_PORT} -d ${DEBUG} -a ${APP_JAR} ${DATACLAY_ARG} -lm ${DATACLAY_EP}
