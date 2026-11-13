FROM eclipse-temurin:21-jdk-noble AS compss-full
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# Bindings + HPC + Orchestrator

# + OpenMPI
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			cmake \
			file \
			g++ \
			libopenmpi-dev \
			make \
			papi-tools \
			psutil \
			python3-dev \
			python3-pip \
			r-base

# + Julia
ARG DOCKER_CE_CLI_VERSION
RUN wget https://download.docker.com/linux/ubuntu/dists/noble/pool/stable/${TARGETARCH}/docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	dpkg -i docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	rm docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin

# + DataClay + Guppy3 + Kafka + MPI4Py + Pytz + Redis
ARG ROCRATE_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			dataclay \
			dill \
			guppy3 \
			kafka-python \
			mpi4py \
			numba \
			pytz \
			redis-py-cluster \
			rocrate==${ROCRATE_VERSION}

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/framework/utils \
	/opt/COMPSs \
	/usr/local/lib/python3.12/dist-packages/compss* \
	/usr/local/lib/python3.12/dist-packages/ear* \
	/usr/local/lib/python3.12/dist-packages/process_affinity* \
	/usr/local/lib/python3.12/dist-packages/pycompss* \
	/

ENV APP_PATH="/app"
ENV LOG_LEVEL="off"
ENV OMPI_ALLOW_RUN_AS_ROOT=1
ENV OMPI_ALLOW_RUN_AS_ROOT_CONFIRM=1

EXPOSE 46101
EXPOSE 46102

SHELL ["/bin/bash", "-lc"]
CMD compss_agent_start --hostname=$(hostname -i) \
					   --classpath="${APP_PATH}" \
					   --log_level="${LOG_LEVEL}" \
					   --log_dir=/log \
					   --rest_port=46101 \
					   --comm_port=46102