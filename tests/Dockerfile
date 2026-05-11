# syntax=docker/dockerfile:1
# In CI, docker_build overrides this with the pre-built registry image so the
# slow apt/pip/julia installs are skipped when tests/Dockerfile hasn't changed.
ARG CI_TEST_DEPS_IMAGE=test-deps

# Stage: pre-install test runner dependencies.
# Built and pushed to the registry by the docker_build_test_deps CI job.
# Only rebuilds when tests/Dockerfile itself changes (apt/pip/julia lists).
FROM eclipse-temurin:21-jdk-noble AS test-deps
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN install -m 0755 -d /etc/apt/keyrings && \
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
	chmod a+r /etc/apt/keyrings/docker.asc && \
	tee /etc/apt/sources.list.d/docker.sources <<-EOF
		Types: deb
		URIs: https://download.docker.com/linux/ubuntu
		Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
		Components: stable
		Signed-By: /etc/apt/keyrings/docker.asc
		EOF

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			automake=1:1.16.5-1.3ubuntu1 \
			bc=1.07.1-3ubuntu4 \
			docker-ce-cli=5:29.4.3-1~ubuntu.24.04~noble \
			file=1:5.45-3build1 \
			g++=4:13.2.0-7ubuntu1 \
			graphviz=2.42.2-9build1 \
			jq=1.7.1-3ubuntu0.24.04.2 \
			libboost-serialization-dev=1.83.0.1ubuntu2 \
			libboost-system-dev=1.83.0.1ubuntu2 \
			libopenmpi-dev=4.1.6-7ubuntu2 \
			make=4.3-4.1build2 \
			openssh-server=1:9.6p1-3ubuntu13.16 \
			python3-dev=3.12.3-0ubuntu2.1 \
			python3-pip=24.0+dfsg-1ubuntu1.3

ARG MAVEN_VERSION=3.9.15
ARG MAVEN_HOME=/opt/maven
RUN curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin && \
	curl -fsSL https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzf - && \
	mv apache-maven-${MAVEN_VERSION} ${MAVEN_HOME}
ENV PATH=${PATH}:${MAVEN_HOME}/bin

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			decorator==5.2.1 \
			dill==0.4.1 \
			guppy3==3.1.6 \
			ipywidgets==8.1.8 \
			jupyterlab==4.5.7 \
			kafka-python==2.3.1 \
			matplotlib==3.10.8 \
			mpi4py==4.1.1 \
			numba==0.65.1 \
			pandas==3.0.2 \
			pytz==2026.2 \
			redis-py-cluster==2.1.3 \
			roc-validator==0.8.1 \
			rocrate==0.14.2 \
			tabulate==0.10.0 \
			types-tabulate==0.10.0.20260408

# Prevent OpenMPI from spinning/hanging in Docker CI.
# COMPSs sets fake SLURM_* env vars for resource control; OpenMPI detects them
# and selects the SLURM PLM launcher, which spins at 100% CPU waiting for a real
# SLURM that doesn't exist in the container. Forcing plm=rsh overrides this.
# btl/oob/btl_tcp settings restrict network probing to loopback only (all worker
# hostnames resolve to 127.0.0.x). Written to openmpi-mca-params.conf so the
# settings apply to SSH-spawned worker processes, not just direct invocations.
# sshd_config SetEnv mirrors the BTL/OOB settings for belt-and-suspenders.
RUN sed -i '/^btl = /d' /etc/openmpi/openmpi-mca-params.conf && \
	printf '\nbtl = tcp,self\nplm = rsh\noob_tcp_if_include = lo\nbtl_tcp_if_include = lo\nbtl_vader_single_copy_mechanism = none\n' >> /etc/openmpi/openmpi-mca-params.conf && \
	printf '\nSetEnv OMPI_MCA_btl=tcp,self OMPI_MCA_oob_tcp_if_include=lo OMPI_MCA_btl_tcp_if_include=lo OMPI_MCA_btl_vader_single_copy_mechanism=none\n' >> /etc/ssh/sshd_config

# Stage: assemble the test runner image from the pre-built deps + COMPSs install.
FROM ${CI_TEST_DEPS_IMAGE} AS tests

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/framework/utils \
	/opt/COMPSs \
	/root/.m2 \
	/usr/local/lib/python3.12/dist-packages/compss* \
	/usr/local/lib/python3.12/dist-packages/process_affinity* \
	/usr/local/lib/python3.12/dist-packages/pycompss* \
	/

COPY . /framework/tests

# Pre-compile all Java test apps so the Maven plugin cache and project deps are
# baked into the image. At test runtime, compilation_and_deployment.py detects
# the pre-built JARs and skips Maven entirely (no network, no re-compilation).
RUN find /framework/tests/sources/local -mindepth 3 -maxdepth 3 -name pom.xml | \
    while IFS= read -r pom; do \
        mvn -f "$pom" install -nsu --no-transfer-progress -q 2>/dev/null || true; \
    done

ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1
RUN ssh-keygen -t rsa -f /root/.ssh/id_rsa -N '' && \
	cat /root/.ssh/id_rsa.pub > /root/.ssh/authorized_keys && \
	cat >> /etc/environment <<-EOT && \
	cat > /compss_entrypoint.sh <<-'EOF' && \
	chmod +x /compss_entrypoint.sh
		JAVA_HOME=${JAVA_HOME}
		OMPI_ALLOW_RUN_AS_ROOT="${OMPI_ALLOW_RUN_AS_ROOT}"
		OMPI_ALLOW_RUN_AS_ROOT_CONFIRM="${OMPI_ALLOW_RUN_AS_ROOT_CONFIRM}"
		EOT
		#!/usr/bin/env -S bash -le
		/framework/tests/scripts/configure_hosts_insert
		service ssh start
		exec /__cacert_entrypoint.sh "$@"
		EOF

ENTRYPOINT ["/compss_entrypoint.sh"]
CMD ["python3", "/framework/tests/scripts/tests.py"]
