FROM eclipse-temurin:21-jdk-noble AS build
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
			libbz2-dev \
			libjpeg-dev \
			liblzma-dev \
			libopenmpi-dev \
			libpcre2-dev \
			libtool \
			libxml2-dev \
			python3-dev \
			python3-pip \
			r-base \
			unzip

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

ARG MYPY_VERSION
RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			dill \
			guppy3 \
			matplotlib \
			memory_profiler \
			mpi4py \
			mypy==${MYPY_VERSION} \
			pytest \
			types-tabulate

COPY dependencies /framework/dependencies

# Git clones must match git submodules from submodules_get.sh
RUN git clone --depth 1 -b next-compss-version https://github.com/stsds/RCOMPSs /framework/compss/programming_model/bindings/RCOMPSs && \
	git clone --depth 1 -b master https://github.com/joblib/threadpoolctl.git /framework/dependencies/threadpoolctl && \
	/framework/dependencies/pre-install-deps

COPY --exclude=dependencies . /framework

RUN . /opt/COMPSs/Dependencies/compss-deps.env && \
	/framework/builders/buildlocal --rcompss

CMD ["/bin/bash", "-l"]
