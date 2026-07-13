FROM eclipse-temurin:21-jdk-noble AS build
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

ARG BUILD_ESSENTIAL_VERSION
ARG CMAKE_VERSION
ARG GFORTRAN_VERSION
ARG GIT_VERSION
ARG LIBBOOST_SERIALIZATION_DEV_VERSION
ARG LIBJPEG_DEV_VERSION
ARG LIBOPENMPI_DEV_VERSION
ARG LIBTOOL_VERSION
ARG LIBXML2_DEV_VERSION
ARG PYTHON3_DEV_VERSION
ARG PYTHON3_PIP_VERSION
ARG R_BASE_VERSION
ARG UNZIP_VERSION
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			build-essential=${BUILD_ESSENTIAL_VERSION} \
			cmake=${CMAKE_VERSION} \
			gfortran=${GFORTRAN_VERSION} \
			git=${GIT_VERSION} \
			libboost-serialization-dev=${LIBBOOST_SERIALIZATION_DEV_VERSION} \
			libjpeg-dev=${LIBJPEG_DEV_VERSION} \
			libopenmpi-dev=${LIBOPENMPI_DEV_VERSION} \
			libtool=${LIBTOOL_VERSION} \
			libxml2-dev=${LIBXML2_DEV_VERSION} \
			python3-dev=${PYTHON3_DEV_VERSION} \
			python3-pip=${PYTHON3_PIP_VERSION} \
			r-base=${R_BASE_VERSION} \
			unzip=${UNZIP_VERSION}

ARG GRADLE_HOME=/opt/gradle
ARG GRADLE_VERSION
ARG MAVEN_HOME=/opt/maven
ARG MAVEN_VERSION
RUN wget https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip && \
	unzip gradle-${GRADLE_VERSION}-bin.zip && \
	rm gradle-${GRADLE_VERSION}-bin.zip && \
	mv gradle-${GRADLE_VERSION} ${GRADLE_HOME} && \
	curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar xzv && \
	mv apache-maven-${MAVEN_VERSION} ${MAVEN_HOME} && \
	ln -s ${MAVEN_HOME}/bin/* /usr/bin

ARG BLACK_JUPYTER_VERSION
ARG DILL_VERSION
ARG GUPPY3_VERSION
ARG MATPLOTLIB_VERSION
ARG MEMORY_PROFILER_VERSION
ARG MPI4PY_VERSION
ARG MYPY_VERSION
ARG PYCODESTYLE_VERSION
ARG PYDOCSTYLE_VERSION
ARG PYTEST_VERSION
ARG TYPES_TABULATE_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			black[jupyter]==${BLACK_JUPYTER_VERSION} \
			dill==${DILL_VERSION} \
			guppy3==${GUPPY3_VERSION} \
			matplotlib==${MATPLOTLIB_VERSION} \
			memory_profiler==${MEMORY_PROFILER_VERSION} \
			mpi4py==${MPI4PY_VERSION} \
			mypy==${MYPY_VERSION} \
			pycodestyle==${PYCODESTYLE_VERSION} \
			pydocstyle==${PYDOCSTYLE_VERSION} \
			pytest==${PYTEST_VERSION} \
			types-tabulate==${TYPES_TABULATE_VERSION}

COPY dependencies /framework/dependencies

ARG DLB_VERSION
ARG EXTRAE_VERSION
RUN cd /framework && \
	git clone --depth 1 -b v${DLB_VERSION} https://gitlab.pm.bsc.es/dlb/dlb.git dependencies/dlb && \
	git clone --depth 1 -b master_compss https://github.com/bsc-wdc/extrae.git dependencies/extrae && \
	git clone --depth 1 -b next-release https://github.com/stsds/RCOMPSs compss/programming_model/bindings/RCOMPSs && \
	git clone --depth 1 -b master https://github.com/joblib/threadpoolctl.git dependencies/threadpoolctl && \
	/framework/dependencies/pre-install-deps

COPY --exclude=dependencies . /framework

RUN . /opt/COMPSs/Dependencies/compss-deps.env && \
	/framework/builders/buildlocal --rcompss

CMD ["/bin/bash"]