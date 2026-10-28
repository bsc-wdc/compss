FROM eclipse-temurin:21-jre-noble AS rcompss
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			cmake \
			r-base

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/opt/COMPSs/Bindings/bindings-common \
	/opt/COMPSs/Bindings/RCOMPSs \
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

RUN mkdir /opt/COMPSs/Runtime/connectors

ENV APP_PATH="/app"
ENV LOG_LEVEL="off"

EXPOSE 46101
EXPOSE 46102

SHELL ["/bin/bash", "-lc"]
CMD compss_agent_start --hostname=$(hostname -i) \
					   --classpath="${APP_PATH}" \
					   --log_level="${LOG_LEVEL}" \
					   --log_dir=/log \
					   --rest_port=46101 \
					   --comm_port=46102

FROM rcompss AS rcompss-hpc

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