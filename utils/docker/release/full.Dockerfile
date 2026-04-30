FROM build AS compss-full
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			file \
			g++ \
			libopenmpi-dev \
			make \
			python3-dev \
			python3-pip \
			r-base

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			dill \
			dataclay \
			guppy3 \
			kafka-python \
			mpi4py \
			numba \
			numpy \
			pytz \
			rocrate