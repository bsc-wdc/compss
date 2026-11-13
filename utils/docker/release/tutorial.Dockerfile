FROM eclipse-temurin:21-jdk-noble AS compss-tutorial
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# Build + Tests

# + Emacs + PAPI + Nano + VIM
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			automake \
			bc \
			build-essential \
			cmake \
			emacs \
			file \
			gfortran \
			git \
			g++ \
			graphviz \
			jq \
			libboost-serialization-dev \
			libboost-system-dev \
			libbz2-dev \
			libjpeg-dev \
			liblzma-dev \
			libopenmpi-dev \
			libpcre2-dev \
			libtool \
			libxml2-dev \
			make \
			nano \
			openssh-server \
			papi-tools \
			python3-dev \
			python3-pip \
			r-base \
			unzip \
			vim

ARG DOCKER_CE_CLI_VERSION
ARG GRADLE_HOME=/opt/gradle
ARG GRADLE_VERSION
ARG MAVEN_HOME=/opt/maven
ARG MAVEN_VERSION
RUN wget https://download.docker.com/linux/ubuntu/dists/noble/pool/stable/${TARGETARCH}/docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	dpkg -i docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	rm docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip && \
	unzip gradle-${GRADLE_VERSION}-bin.zip && \
	rm gradle-${GRADLE_VERSION}-bin.zip && \
	mv gradle-${GRADLE_VERSION} ${GRADLE_HOME} && \
	curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin && \
	curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzv && \
	mv apache-maven-${MAVEN_VERSION} ${MAVEN_HOME} && \
	ln -s ${MAVEN_HOME}/bin/* /usr/bin

# + DataClay + Dislib + Notebook + PyCOMPSs-CLI + Qdislib
ARG MYPY_VERSION
ARG ROC_VALIDATOR_VERSION
ARG ROCRATE_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			dataclay \
			decorator \
			dill \
    		dislib \
			guppy3 \
			ipywidgets \
			jupyterlab \
			kafka-python \
			matplotlib \
			memory_profiler \
			mpi4py \
			mypy==${MYPY_VERSION} \
			notebook \
			numba \
			pandas \
			psutil \
			pycompss-cli \
			pytest \
			pytz \
    		qdislib \
			redis \
			roc-validator==${ROC_VALIDATOR_VERSION} \
			rocrate==${ROCRATE_VERSION} \
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
		service ssh start
		exec /__cacert_entrypoint.sh "$@"
		EOF

ENTRYPOINT ["/compss_entrypoint.sh"]
CMD ["/bin/bash"]