FROM eclipse-temurin:21-jre AS compss-orchestra
ARG DEBIAN_FRONTEND=noninteractive
ARG TARGETARCH

RUN install -m 0755 -d /etc/apt/keyrings && \
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc && \
	chmod a+r /etc/apt/keyrings/docker.asc && \
	tee /etc/apt/sources.list.d/docker.sources <<-EOF
		Types: deb
		URIs: https://download.docker.com/linux/ubuntu
		Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
		Components: stable
		Signed-By: /etc/apt/keyrings/docker.asc
		EOF

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked,id=apt-${TARGETARCH} \
	--mount=type=cache,target=/var/lib/apt,sharing=locked,id=libapt-${TARGETARCH} \
	rm -f /etc/apt/apt.conf.d/docker-clean && \
	echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache && \
	apt-get update && \
	apt-get install -y --no-install-recommends \
			docker-ce-cli \
			uuid-runtime

COPY --from=build --link --parents \
	/etc/profile.d/compss.sh \
	/opt/COMPSs/Runtime/adaptors/CommAgent \
	/opt/COMPSs/Runtime/adaptors/http \
	/opt/COMPSs/Runtime/adaptors/local \
	/opt/COMPSs/Runtime/adaptors/RESTagent \
	/opt/COMPSs/Runtime/checkpointer/compss-checkpointer-no-checkpoint.jar \
	/opt/COMPSs/Runtime/compss-agent-impl.jar \
	/opt/COMPSs/Runtime/compss-engine.jar \
	/opt/COMPSs/Runtime/configuration/agents/all.json \
	/opt/COMPSs/Runtime/configuration/xml/projects/examples/local/project.xml \
	/opt/COMPSs/Runtime/configuration/xml/projects/project_schema.xsd \
	/opt/COMPSs/Runtime/configuration/xml/resources/examples/local/resources.xml \
	/opt/COMPSs/Runtime/configuration/xml/resources/resources_schema.xsd \
	/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j \
	/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j.debug \
	/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j.info \
	/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j.off \
	/opt/COMPSs/Runtime/scheduler \
	/opt/COMPSs/Runtime/scripts/user/compss_agent_start \
	/opt/COMPSs/Runtime/scripts/system/agents \
	/opt/COMPSs/Runtime/scripts/system/commons \
	/opt/COMPSs/Runtime/scripts/system/runtime \
	/opt/COMPSs/Runtime/scripts/system/trace \
	/

RUN mkdir /opt/COMPSs/Runtime/connectors

ENV APP_PATH="/app"
ENV LOG_LEVEL="off"
ENV SOCKET="/tmp/compss.sock"

EXPOSE 46101
EXPOSE 46102

ENTRYPOINT ["/__cacert_entrypoint.sh"]
SHELL ["/bin/bash", "-lc"]
CMD compss_agent_start --hostname=$(hostname -i) \
					   --classpath="${APP_PATH}" \
					   --log_level="${LOG_LEVEL}" \
					   --log_dir=/log \
					   --socket="${SOCKET}" \
					   --rest_port=46101 \
					   --comm_port=46102

HEALTHCHECK CMD curl -f http://$(hostname -i):46101/COMPSs/test || exit 1