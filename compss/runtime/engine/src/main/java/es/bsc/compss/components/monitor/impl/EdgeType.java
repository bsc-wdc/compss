package es.bsc.compss.components.monitor.impl;

public enum EdgeType {
    DATA_DEPENDENCY(""), // Regular data dependencies
    USER_DEPENDENCY("color=grey"), // For user synchronisations (barriers, api calls, etc.)
    STREAM_DEPENDENCY("style=dashed"); // Stream data dependencies

    private final String properties;


    private EdgeType(String properties) {
        this.properties = properties;
    }

    public String getProperties() {
        return this.properties;
    }

}