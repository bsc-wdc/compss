FROM eclipse-temurin:21-jdk-noble AS compss-full
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

# Bindings + HPC + Orchestra

# + OpenMPI
ARG CMAKE_VERSION
ARG GPP_VERSION
ARG LIBOPENMPI_DEV_VERSION
ARG MAKE_VERSION
ARG PAPI_TOOLS_VERSION
ARG PYTHON3_VERSION
ARG PYTHON3_PIP_VERSION
ARG R_BASE_VERSION
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			cmake=${CMAKE_VERSION} \
			g++=${GPP_VERSION} \
			libopenmpi-dev=${LIBOPENMPI_DEV_VERSION} \
			make=${MAKE_VERSION} \
			papi-tools=${PAPI_TOOLS_VERSION} \
			python3=${PYTHON3_VERSION} \
			python3-pip=${PYTHON3_PIP_VERSION} \
			r-base=${R_BASE_VERSION}

# + Julia
ARG DOCKER_CE_CLI_VERSION
RUN wget https://download.docker.com/linux/ubuntu/dists/noble/pool/stable/${TARGETARCH}/docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	dpkg -i docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	rm docker-ce-cli_${DOCKER_CE_CLI_VERSION}_${TARGETARCH}.deb && \
	curl -fsSL https://install.julialang.org | sh -s -- -y && \
	ln -s /root/.juliaup/bin/* /usr/bin

# + DataClay + Guppy3 + Kafka + MPI4Py + Pytz + Redis
ARG DATACLAY_VERSION
ARG DILL_VERSION
ARG GUPPY3_VERSION
ARG KAFKA_PYTHON_VERSION
ARG MPI4PY_VERSION
ARG NUMBA_VERSION
ARG PYTZ_VERSION
ARG REDIS_PY_CLUSTER_VERSION
ARG ROCRATE_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			dataclay==${DATACLAY_VERSION} \
			dill==${DILL_VERSION} \
			guppy3==${GUPPY3_VERSION} \
			kafka-python==${KAFKA_PYTHON_VERSION} \
			mpi4py==${MPI4PY_VERSION} \
			numba==${NUMBA_VERSION} \
			pytz==${PYTZ_VERSION} \
			redis-py-cluster==${REDIS_PY_CLUSTER_VERSION} \
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