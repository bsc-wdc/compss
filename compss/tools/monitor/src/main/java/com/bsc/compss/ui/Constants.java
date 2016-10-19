package com.bsc.compss.ui;

import java.io.File;


public class Constants {

    // ZUL Files structure
    public static final String MAIN_PAGE = File.separator + "index.zul";
    public static final String LOGIN_PAGE = File.separator + "zul" + File.separator + "login.zul";

    // Monitor Files structure
    public static final String MONITOR_XML_FILE = File.separator + "monitor" + File.separator + "COMPSs_state.xml";
    public static final String MONITOR_CURRENT_DOT_FILE = File.separator + "monitor" + File.separator + "current_graph.dot";
    public static final String MONITOR_COMPLETE_DOT_FILE = File.separator + "monitor" + File.separator + "complete_graph.dot";
    public static final String GRAPH_FILE_NAME = "graph.svg";
    public static final String COMPLETE_GRAPH_FILE_NAME = "completeGraph.svg";
    public static final String GRAPH_NOT_FOUND_PATH = File.separator + "svg" + File.separator + "graph_not_found.svg";
    public static final String EMPTY_GRAPH_PATH = File.separator + "svg" + File.separator + "empty_graph.svg";
    public static final String UNSELECTED_GRAPH_PATH = File.separator + "svg" + File.separator + "unselected_graph.svg";
    public static final String GRAPH_EXECUTION_DONE_PATH = File.separator + "svg" + File.separator + "graph_execution_done.svg";
    public static final String NO_CONNECTION_IMG_PATH = File.separator + "compss-monitor" + File.separator + "svg" + File.separator
            + "loadGraphNoConnection.png";
    public static final String IT_MONITOR_DEFAULT = File.separator + "root" + File.separator + ".COMPSs" + File.separator;

    // Runtime files structure
    public static final String RUNTIME_LOG = "runtime.log";
    public static final String RESOURCES_LOG = "resources.log";
    public static final String JOBS_SUB_PATH = File.separator + "jobs" + File.separator;
    public static final String JOB_OUT_FILE = "_NEW.out";
    public static final String JOB_OUT_RESUBMITTED_FILE = "_RESUBMITTED.out";
    public static final String JOB_ERR_FILE = "_NEW.err";
    public static final String JOB_ERR_RESUBMITTED_FILE = "_RESUBMITTED.err";

    // Default & environment usernames
    public static final String USER_DEFAULT = "default";
    public static final String USER_ENVIRONMENT = "environment";
    public static final String USER_DIRECT_PATH = "path";

    // Resource status
    public static final String STATUS_CREATION = "On Creation";
    public static final String STATUS_RUNNING = "Running";
    public static final String STATUS_REMOVING = "On Destroy";
    public static final String STATUS_TASK_CREATING = "Creating";
    public static final String STATUS_TASK_RUNNING = "Running";
    public static final String STATUS_TASK_DONE = "Done";
    public static final String STATUS_TASK_FAILED = "Failed";
    public static final String COLOR_CREATION = "yellow";
    public static final String COLOR_RUNNING = "green";
    public static final String COLOR_REMOVING = "red";
    public static final String COLOR_TASK_CREATING = "yellow";
    public static final String COLOR_TASK_RUNNING = "blue";
    public static final String COLOR_TASK_DONE = "green";
    public static final String COLOR_TASK_FAILED = "red";

    // Chart types
    public static final String TOTAL_LOAD_CHART = "totalLoadChart"; // According to monitor.zul (lc tab)
    public static final String LOAD_PER_CORE_CHART = "loadPerCoreChart";
    public static final String TOTAL_RUNNING_CHART = "totalRunningChart";
    public static final String RUNNING_PER_CORE_CHART = "runningPerCoreChart";
    public static final String TOTAL_PENDING_CHART = "totalPendingChart";
    public static final String PENDING_PER_CORE_CHART = "pendingPerCoreChart";
    public static final String RESOURCES_STATUS_CHART = "totalResourcesStatusChart";

    // ZUL Tab names
    public static final String resourcesInformationTabName = "resourcesInformationTab"; // According to monitor.zul
    public static final String tasksInformationTabName = "tasksInformationTab";
    public static final String currentTasksGraphTabName = "currentTasksGraphTab";
    public static final String completeTasksGraphTabName = "completeTasksGraphTab";
    public static final String loadChartTabName = "loadChartTab";
    public static final String runtimeLogTabName = "runtimeLogTab";
    public static final String executionInformationTabName = "executionInformationTab";
    public static final String statisticsTabName = "statisticsTab";

    // ZUL execution information labels
    public static final String EIL_ALL = "all"; // According to monitor.zul (ei tab)
    public static final String EIL_CURRENT_TASKS = "current_tasks";
    public static final String EIL_FAILED_TASKS = "failed_tasks";
    public static final String EIL_TASKS_WITH_FAILED_JOBS = "tasks_with_failed_jobs";

    // Core colors
    public static final int CORE_COLOR_DEFAULT = 0; // Blue
    public static final int CORE_COLOR_MAX = 24; // #images on color folder. According to Colors.java class

}
