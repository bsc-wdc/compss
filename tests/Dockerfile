FROM eclipse-temurin:21-jdk-noble AS tests
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

ARG AUTOMAKE_VERSION
ARG BC_VERSION
ARG FILE_VERSION
ARG GPP_VERSION
ARG GRAPHVIZ_VERSION
ARG JQ_VERSION
ARG LIBBOOST_SERIALIZATION_DEV_VERSION
ARG LIBBOOST_SYSTEM_DEV_VERSION
ARG LIBOPENMPI_DEV_VERSION
ARG MAKE_VERSION
ARG OPENSSH_SERVER_VERSION
ARG PYTHON3_DEV_VERSION
ARG PYTHON3_PIP_VERSION
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			automake=${AUTOMAKE_VERSION} \
			bc=${BC_VERSION} \
			file=${FILE_VERSION} \
			g++=${GPP_VERSION} \
			graphviz=${GRAPHVIZ_VERSION} \
			jq=${JQ_VERSION} \
			libboost-serialization-dev=${LIBBOOST_SERIALIZATION_DEV_VERSION} \
			libboost-system-dev=${LIBBOOST_SYSTEM_DEV_VERSION} \
			libopenmpi-dev=${LIBOPENMPI_DEV_VERSION} \
			make=${MAKE_VERSION} \
			openssh-server=${OPENSSH_SERVER_VERSION} \
			python3-dev=${PYTHON3_DEV_VERSION} \
			python3-pip=${PYTHON3_PIP_VERSION}

ARG DOCKER_CE_CLI_VERSION
ARG MAVEN_HOME=/opt/maven
ARG MAVEN_VERSION
RUN wget https://download.docker.com/linux/ubuntu/dists/noble/pool/stable/${TARGETARCH}/docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	dpkg -i docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	rm docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin && \
	curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzv && \
	mv apache-maven-${MAVEN_VERSION} ${MAVEN_HOME} && \
	ln -s ${MAVEN_HOME}/bin/* /usr/bin

ARG DECORATOR_VERSION
ARG DILL_VERSION
ARG GUPPY3_VERSION
ARG IPYWIDGETS_VERSION
ARG JUPYTERLAB_VERSION
ARG KAFKA_PYTHON_VERSION
ARG MATPLOTLIB_VERSION
ARG MPI4PY_VERSION
ARG NUMBA_VERSION
ARG PANDAS_VERSION
ARG PYTZ_VERSION
ARG REDIS_VERSION
ARG ROC_VALIDATOR_VERSION
ARG ROCRATE_VERSION
ARG TABULATE_VERSION
ARG TYPES_TABULATE_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			decorator==${DECORATOR_VERSION} \
			dill==${DILL_VERSION} \
			guppy3==${GUPPY3_VERSION} \
			ipywidgets==${IPYWIDGETS_VERSION} \
			jupyterlab==${JUPYTERLAB_VERSION} \
			kafka-python==${KAFKA_PYTHON_VERSION} \
			matplotlib==${MATPLOTLIB_VERSION} \
			mpi4py==${MPI4PY_VERSION} \
			numba==${NUMBA_VERSION} \
			pandas==${PANDAS_VERSION} \
			pytz==${PYTZ_VERSION} \
			redis==${REDIS_VERSION} \
			roc-validator==${ROC_VALIDATOR_VERSION} \
			rocrate==${ROCRATE_VERSION} \
			tabulate==${TABULATE_VERSION} \
			types-tabulate==${TYPES_TABULATE_VERSION}

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
RUN mvn -f /framework/tests/sources/local -nsu -ntp -q install && \
	ssh-keygen -t rsa -f /root/.ssh/id_rsa -N '' && \
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

ENV KAFKA_REPLICATION_FACTOR=1

ENTRYPOINT ["/compss_entrypoint.sh"]
CMD ["python3", "/framework/tests/scripts/tests.py"]