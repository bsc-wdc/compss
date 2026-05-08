FROM eclipse-temurin:21-jdk-noble AS tests
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
			automake \
			bc \
			docker-ce-cli \
			file \
			g++ \
			graphviz \
			jq \
			libboost-serialization-dev \
			libboost-system-dev \
			libopenmpi-dev \
			make \
			openssh-server \
			python3-dev \
			python3-pip

ARG MAVEN_VERSION=3.9.15
ARG MAVEN_HOME=/opt/maven
RUN curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin && \
	curl -fsSL https://dlcdn.apache.org/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzv && \
	mv apache-maven-${MAVEN_VERSION} ${MAVEN_HOME}
ENV PATH=${PATH}:${MAVEN_HOME}/bin

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			decorator \
			dill \
			guppy3 \
			ipywidgets \
			jupyterlab \
			kafka-python \
			matplotlib \
			mpi4py \
			numba \
			pandas \
			pytz \
			redis-py-cluster \
			roc-validator==0.8.1 \
			rocrate==0.14.2 \
			tabulate \
			types-tabulate

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/framework/utils \
	/opt/COMPSs \
	/root/.m2 \
	/usr/local/lib/python3.12/dist-packages/compss* \
	/usr/local/lib/python3.12/dist-packages/ear* \
	/usr/local/lib/python3.12/dist-packages/process_affinity* \
	/usr/local/lib/python3.12/dist-packages/pycompss* \
	/

COPY . /framework/tests

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