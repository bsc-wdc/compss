ARG DEBIAN_FRONTEND=noninteractive
ARG BASE=base22
# TODO: define BASE_VERSION whenever a new release is done
ARG BASE_VERSION=260305-135057
# In CI, docker_build overrides this with the pre-built registry image so the
# deps stage is bypassed entirely (no submodule init or compilation needed).
ARG CI_DEPS_IMAGE=deps

# Stage: pre-install external dependencies (Extrae, DLB, Kafka, Tomcat, JaCoCo).
# Built and pushed to the registry by the docker_build_deps CI job.
# Clones submodules directly from their remotes (no .git needed in context).
FROM compss/${BASE}_ci:${BASE_VERSION} AS deps

ENV GRADLE_HOME=/opt/gradle

COPY . /framework

RUN cd /framework && \
    git clone --depth=1 --branch master_compss https://github.com/bsc-wdc/extrae.git dependencies/extrae && \
    git clone --depth=1 --branch v3.6.0 https://gitlab.pm.bsc.es/dlb/dlb.git dependencies/dlb && \
    git clone --depth=1 --branch master https://github.com/joblib/threadpoolctl.git dependencies/threadpoolctl && \
    git clone --depth=1 --branch next-release https://github.com/stsds/RCOMPSs compss/programming_model/bindings/RCOMPSs && \
    /framework/builders/pre-install-deps --install-dir=/opt/COMPSs-deps

# Stage: build and install COMPSs.
# Starts FROM the pre-built deps registry image when CI_DEPS_IMAGE is set,
# skipping the deps stage above entirely.
FROM ${CI_DEPS_IMAGE} AS ci

COPY . /framework

ENV PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/opt/gradle/bin
ENV CLASSPATH=/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs

# Install COMPSs (source the pre-installed deps env so buildlocal picks up
# EXTRAE_HOME, DLB_HOME, KAFKA_HOME, TOMCAT_HOME, JACOCO_HOME).
# The Maven cache mount persists /root/.m2 across builds on the same host so
# Maven artifacts are not re-downloaded on every commit.
RUN --mount=type=cache,target=/root/.m2 cd /framework && \
    python3 -m pip --no-cache-dir install pip wheel setuptools kafka-python && \
    . /opt/COMPSs-deps/compss-deps.env && \
    /framework/builders/buildlocal --quiet --skip-tests --no-pycompss-compile --no-python-style --rcompss /opt/COMPSs && \
    cp -r /root/.m2 /home/jenkins && \
    chown -R jenkins: /framework /home/jenkins/

# Expose SSH port and run SSHD
EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_all:${BASE_VERSION} AS compss

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH=/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/

EXPOSE 22
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_tutorial:${BASE_VERSION} AS compss-tutorial

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils:/root/.local/bin
ENV CLASSPATH=/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/
ENV PYTHONPATH=$COMPSS_HOME/Bindings/python/3:$PYTHONPATH
ARG TZ=Etc/UTC

RUN python3 -m pip install "setuptools<70" wheel hatchling hatch hatch-nodejs-version hatch-jupyter-builder --upgrade --force-reinstall && \
    python3 -m pip install --no-cache-dir --no-build-isolation dislib pycompss-cli && \
    git clone https://github.com/bsc-wdc/jupyter-extension.git je && \
    cd je && sed -i '/\"pycompss\"/d' ipycompss_kernel/pyproject.toml && \
    python3 -m pip install --no-build-isolation ./ipycompss_kernel && cd ipycompss_lab_extension && \
    jlpm install --network-timeout 600000 --network-concurrency 100 && \
    jlpm run build:prod && python3 -m pip --no-cache-dir install --no-build-isolation . && cd ../.. && rm -r je

EXPOSE 22
EXPOSE 43000-44000
CMD ["/usr/sbin/sshd","-D"]

FROM compss/${BASE}_rt:${BASE_VERSION} AS minimal

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH=/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/


FROM compss/${BASE}_bindings:${BASE_VERSION} AS pycompss

COPY --from=ci /opt/COMPSs /opt/COMPSs
COPY --from=ci /etc/init.d/compss-monitor /etc/init.d/compss-monitor
COPY --from=ci /etc/profile.d/compss.sh /etc/profile.d/compss.sh

ENV PATH=$PATH:/opt/COMPSs/Runtime/scripts/user:/opt/COMPSs/Bindings/c/bin:/opt/COMPSs/Runtime/scripts/utils
ENV CLASSPATH=/opt/COMPSs/Runtime/compss-engine.jar
ENV LD_LIBRARY_PATH=/opt/COMPSs/Bindings/bindings-common/lib:$LD_LIBRARY_PATH
ENV COMPSS_HOME=/opt/COMPSs/
