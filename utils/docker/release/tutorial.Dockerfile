FROM compss-full AS tutorial
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			automake \
			build-essential \
			emacs \
			gfortran \
			git \
			graphviz \
			libtool \
			libxml2-dev \
			nano \
			unzip \
			vim

RUN --mount=type=cache,target=/root/.cache/pip,id=pip-${TARGETARCH} \
	python3 -m pip install --break-system-packages \
			black \
			black[jupyter] \
			decorator \
			ipykernel \
			ipython \
			ipywidgets \
			jupyterlab \
			matplotlib \
			memory_profiler \
			pandas \
			pycompss-cli \
			pycodestyle \
			pydocstyle \
			pytest \
			redis-py-cluster \
			roc-validator \
			scikit-learn \
			scipy \
			tabulate \
			types-tabulate