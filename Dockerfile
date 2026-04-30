FROM eclipse-temurin:21-jdk AS build
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			build-essential \
			cmake \
			gfortran \
			git \
			libboost-serialization-dev \
			libjpeg-dev \
			libopenmpi-dev \
			libtool \
			libxml2-dev \
			maven \
			python3-dev \
			python3-pip \
			r-base \
			unzip

ARG GRADLE_HOME=/opt/gradle
RUN wget https://services.gradle.org/distributions/gradle-8.7-bin.zip && \
	unzip gradle-8.7-bin.zip && \
	rm gradle-8.7-bin.zip && \
	mv gradle-8.7 ${GRADLE_HOME}

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			black[jupyter] \
			dill \
			guppy3 \
			matplotlib \
			memory_profiler \
			mpi4py \
			mypy \
			pycodestyle \
			pydocstyle \
			pytest \
			types-tabulate

COPY --parents \
	builders/pre-install-deps \
	dependencies/install_dlb.sh  \
	dependencies/install_extrae.sh \
	/framework

RUN cd /framework && \
	git clone --depth 1 -b v3.6.0 https://gitlab.pm.bsc.es/dlb/dlb.git dependencies/dlb && \
	git clone --depth 1 -b master_compss https://github.com/bsc-wdc/extrae.git dependencies/extrae && \
	git clone --depth 1 -b next-release https://github.com/stsds/RCOMPSs compss/programming_model/bindings/RCOMPSs && \
	git clone --depth 1 -b master https://github.com/joblib/threadpoolctl.git dependencies/threadpoolctl && \
	/framework/builders/pre-install-deps

COPY . /framework

RUN . /opt/COMPSs/Dependencies/compss-deps.env && \
	/framework/builders/buildlocal --rcompss

CMD ["/bin/bash"]