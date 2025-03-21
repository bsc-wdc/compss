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
package es.bsc.compss.ui;

import es.bsc.compss.exceptions.NonInstantiableException;


public class Properties {

    private static final String DEFAULT_BASE_PATH = "";
    private static final int DEFAULT_REFRESH_TIME = 5_000;
    private static final boolean DEFAULT_SORT_APPLICATIONS = true;
    private static final int DEFAULT_X_SCALE_FOR_LOAD_GRAPH = 1;

    // Base path of current monitored application
    private static String basePath = "";

    // Monitor properties state
    private static int refreshTime = 5_000; // MiliSeconds
    private static boolean sortApplications = true; // true or false
    private static int xScaleForLoadGraph = 1; // Int >= 1

    static {
        basePath = DEFAULT_BASE_PATH;
        refreshTime = DEFAULT_REFRESH_TIME;
        sortApplications = DEFAULT_SORT_APPLICATIONS;
        xScaleForLoadGraph = DEFAULT_X_SCALE_FOR_LOAD_GRAPH;
    }


    private Properties() {
        throw new NonInstantiableException("Properties");
    }

    public static String getBasePath() {
        return basePath;
    }

    public static int getRefreshTime() {
        return refreshTime;
    }

    public static boolean isSortApplications() {
        return sortApplications;
    }

    public static int getxScaleForLoadGraph() {
        return xScaleForLoadGraph;
    }

    public static void setBasePath(String basePath) {
        Properties.basePath = basePath;
    }

    public static void setRefreshTime(int refreshTime) {
        Properties.refreshTime = refreshTime;
    }

    public static void setSortApplications(boolean sortApplications) {
        Properties.sortApplications = sortApplications;
    }

    public static void setxScaleForLoadGraph(int xScaleForLoadGraph) {
        Properties.xScaleForLoadGraph = xScaleForLoadGraph;
    }

}
