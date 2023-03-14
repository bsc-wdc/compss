ARG DEBIAN_FRONTEND=noninteractive
ARG BASE=base20
ARG BASE_VERSION=latest

FROM compss/${BASE}_ci:${BASE_VERSION} as ci
ENV GRADLE_HOME /opt/gradle
ENV PATH $PATH:/opt/gradle/bin

COPY . /framework

ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/opt/gradle/bin
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/

# Install COMPSs
RUN cd /framework && \
    ./submodules_get.sh && \
    #export EXTRAE_MPI_HEADERS=/usr/include/x86_64-linux-gnu/mpi && \
    /framework/builders/buildlocal /opt/COMPSs && \
    mv /root/.m2 /home/jenkins && \
    chown -R jenkins: /framework && \
    chown -R jenkins: /home/jenkins/ 

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_all:${BASE_VERSION} as compss

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/

EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_tutorial:${BASE_VERSION} as compss-tutorial

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

#ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/root/.local/bin
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/
ENV PYTHONPATH=$COMPSS_HOME/Bindings/python/3:$PYTHONPATH

RUN python3 -m pip install --no-cache-dir dislib

EXPOSE 22
EXPOSE 43000-44000
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_rt:${BASE_VERSION} as minimal

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/


FROM compss/${BASE}_python:${BASE_VERSION} as pycompss 

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/

