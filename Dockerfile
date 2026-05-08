FROM eclipse-temurin:21-jdk-noble AS build
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			build-essential=12.10ubuntu1 \
			cmake=3.28.3-1build7 \
			gfortran=4:13.2.0-7ubuntu1 \
			git=1:2.43.0-1ubuntu7.3 \
			libboost-serialization-dev=1.83.0.1ubuntu2 \
			libjpeg-dev=8c-2ubuntu11 \
			libopenmpi-dev=4.1.6-7ubuntu2 \
			libtool=2.4.7-7build1 \
			libxml2-dev=2.9.14+dfsg-1.3ubuntu3.7 \
			maven=3.8.7-2 \
			python3-dev=3.12.3-0ubuntu2.1 \
			python3-pip=24.0+dfsg-1ubuntu1.3 \
			r-base=4.3.3-2build2 \
			unzip=6.0-28ubuntu4.1

ARG GRADLE_HOME=/opt/gradle
RUN wget https://services.gradle.org/distributions/gradle-8.7-bin.zip && \
	unzip gradle-8.7-bin.zip && \
	rm gradle-8.7-bin.zip && \
	mv gradle-8.7 ${GRADLE_HOME}

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			black[jupyter]==26.3.1 \
			dill==0.4.1 \
			guppy3==3.1.6 \
			matplotlib==3.10.8 \
			memory_profiler==0.61.0 \
			mpi4py==4.1.1 \
			mypy==1.20.2 \
			pycodestyle==2.14.0 \
			pydocstyle==6.3.0 \
			pytest==9.0.3 \
			types-tabulate==0.10.0.20260408

COPY --parents \
	builders/pre-install-deps \
	dependencies/install_dlb.sh  \
	dependencies/install_extrae.sh \
	/framework/

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