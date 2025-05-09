ARG DEBIAN_FRONTEND=noninteractive
ARG BASE=base22
ARG BASE_VERSION=250509-135237

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
    python3 -m pip --no-cache-dir install pip wheel setuptools --upgrade && \
    /framework/builders/buildlocal --rcompss /opt/COMPSs && \
    mv /root/.m2 /home/jenkins && \
    chown -R jenkins: /framework && \
    chown -R jenkins: /home/jenkins/ && \
    python3 -m pip install --no-cache-dir rocrate==0.9.0 roc-validator==0.4.2


# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_all:${BASE_VERSION} as compss

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

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

ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/root/.local/bin
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/
ENV PYTHONPATH=$COMPSS_HOME/Bindings/python/3:$PYTHONPATH
ARG TZ=Etc/UTC

RUN python3 -m pip install --no-cache-dir dislib pycompss-cli && \
    git clone https://github.com/bsc-wdc/jupyter-extension.git je && \
    cd je && sed -i '/\"pycompss\"/d' ipycompss_kernel/pyproject.toml && \
    python3 -m pip install ./ipycompss_kernel && cd ipycompss_lab_extension && \
    jlpm install --network-timeout 600000 --network-concurrency 100 && \
    jlpm run build:prod && python3 -m pip --no-cache-dir install . && cd ../.. && rm -r je

EXPOSE 22
EXPOSE 43000-44000
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_rt:${BASE_VERSION} as minimal

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/


FROM compss/${BASE}_python:${BASE_VERSION} as pycompss

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH $PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH $CLASSPATH:/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH /opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/
