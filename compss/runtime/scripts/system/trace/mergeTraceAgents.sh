java \
    -cp ${COMPSS_HOME}/Runtime/adaptors/CommAgent/worker/compss-adaptors-agent-comm-worker.jar:${COMPSS_HOME}/Runtime/adaptors/RESTAgent/worker/compss-adaptors-agent-rest-worker.jar:${COMPSS_HOME}/Runtime/compss-engine.jar:${COMPSS_HOME}/Runtime/compss-agent-impl.jar\
    es.bsc.compss.agent.TraceManager "$@"
