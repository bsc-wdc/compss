/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.commons;

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
