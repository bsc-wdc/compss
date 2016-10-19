package com.bsc.compss.commons;

public class Loggers {

    public static final String COMPSS_MONITOR = "compssMonitor";

    public static final String BESFactory = COMPSS_MONITOR + ".BESFactoryPort";

    public static final String UI_AUTHENTICATION = COMPSS_MONITOR + ".Authentication";

    public static final String UI_VMS = COMPSS_MONITOR + ".VM";
    public static final String UI_VM_APPLICATIONS = UI_VMS + ".ApplicationsVM";
    public static final String UI_VM_RESOURCES = UI_VMS + ".ResourcesVM";
    public static final String UI_VM_TASKS = UI_VMS + ".TasksVM";
    public static final String UI_VM_GRAPH = UI_VMS + ".GraphVM";
    public static final String UI_VM_LOAD_CHART = UI_VMS + ".LoadChartVM";
    public static final String UI_VM_RUNTIME_LOG = UI_VMS + ".RuntimeLogVM";
    public static final String UI_VM_EXEC_INFO = UI_VMS + ".ExecutionInformationVM";
    public static final String UI_VM_CONFIGURATION = UI_VMS + ".ConfigurationVM";
    public static final String UI_VM_STATISTICS = UI_VMS + ".StatisticsVM";

    public static final String PARSERS = COMPSS_MONITOR + ".Parsers";
    public static final String COMPSS_STATE_XML_PARSER = PARSERS + ".COMPSsStateXML";
    public static final String RUNTIME_LOG_PARSER = PARSERS + ".RuntimeLog";
    public static final String RESOURCES_LOG_PARSER = PARSERS + ".ResourcesLog";

}
