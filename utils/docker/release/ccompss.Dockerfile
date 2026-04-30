FROM compss-orch AS ccompss
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
			make \
			openssh-server \
			uuid-runtime

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/opt/COMPSs/Bindings/bindings-common \
	/opt/COMPSs/Bindings/c \
	/opt/COMPSs/LICENSE \
	/opt/COMPSs/NOTICE \
	/opt/COMPSs/Runtime/adaptors/CommAgent \
	/opt/COMPSs/Runtime/adaptors/http \
	/opt/COMPSs/Runtime/adaptors/local \
	/opt/COMPSs/Runtime/adaptors/RESTagent \
	/opt/COMPSs/Runtime/checkpointer \
	/opt/COMPSs/Runtime/compss-agent-impl.jar \
	/opt/COMPSs/Runtime/compss-engine.jar \
	/opt/COMPSs/Runtime/configuration \
	/opt/COMPSs/Runtime/scheduler \
	/opt/COMPSs/Runtime/scripts \
	/

COPY --chmod=755 <<-'EOF' /compss_entrypoint.sh
	#!/usr/bin/env -S bash -le
	service ssh start
	exec /__cacert_entrypoint.sh "$@"
	EOF

ENV APP_PATH="/app"
ENV LOG_LEVEL="off"

EXPOSE 22
EXPOSE 46101
EXPOSE 46102

ENTRYPOINT ["/compss_entrypoint.sh"]
SHELL ["/bin/bash", "-lc"]
CMD compss_agent_start --hostname=$(hostname -i) \
					   --classpath="${APP_PATH}" \
					   --log_level="${LOG_LEVEL}" \
					   --log_dir=/log \
					   --rest_port=46101 \
					   --comm_port=46102

FROM ccompss AS ccompss-hpc

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			papi-tools

COPY --from=build --link --parents \
	/opt/COMPSs/Dependencies/dlb \
	/opt/COMPSs/Dependencies/extrae \
	/opt/COMPSs/Runtime/adaptors \
	/opt/COMPSs/Tools/tracing \
	/