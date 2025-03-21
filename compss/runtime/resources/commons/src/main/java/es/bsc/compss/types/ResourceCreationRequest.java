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
package es.bsc.compss.types;

import es.bsc.compss.types.listeners.ResourceCreationListener;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.util.CoreManager;

import org.apache.logging.log4j.Logger;


public class ResourceCreationRequest {

    private final CloudProvider provider;
    private final CloudMethodResourceDescription requested;
    private int[][] requestedSimultaneousTaskCount;
    private final long requestedTime;
    private String requestID; // It will be the same as the VM name
    private ResourceCreationListener listener;


    /**
     * Creates a new ResourceCreationRequest instance.
     * 
     * @param requestedResource Description of the requested resource.
     * @param simultaneousTasks Simultaneous tasks that are required to run in the created resource.
     * @param cp Associated Cloud Provider.
     * @param requestID Request Id.
     * @param listener listener to be notified progress on the resource creation
     */
    public ResourceCreationRequest(CloudMethodResourceDescription requestedResource, int[][] simultaneousTasks,
        CloudProvider cp, String requestID, ResourceCreationListener listener) {

        this.requested = requestedResource;
        this.provider = cp;
        this.requestedSimultaneousTaskCount = simultaneousTasks;
        this.requestedTime = System.currentTimeMillis();
        this.requestID = requestID;
        this.listener = listener;
    }

    /**
     * Returns the requested time.
     * 
     * @return The requested time.
     */
    public long getRequestedTime() {
        return this.requestedTime;
    }

    /**
     * Returns the requested resource description.
     * 
     * @return The requested resource description.
     */
    public CloudMethodResourceDescription getRequested() {
        return this.requested;
    }

    /**
     * Returns the associated Cloud Provider.
     * 
     * @return The associated Cloud Provider.
     */
    public CloudProvider getProvider() {
        return this.provider;
    }

    /**
     * Returns the request Id.
     * 
     * @return The request Id.
     */
    public String getRequestID() {
        return this.requestID;
    }

    /**
     * Returns the request's listener.
     * 
     * @return The request's listener.
     */
    public ResourceCreationListener getListener() {
        return this.listener;
    }

    /**
     * Returns the requested simultaneous task count.
     * 
     * @return The requested simultaneous task count.
     */
    public int[][] requestedSimultaneousTaskCount() {
        return this.requestedSimultaneousTaskCount;
    }

    /**
     * Updates the simultaneous task count.
     * 
     * @param newRequestedSimultaneousTaskCount New simultaneous task count.
     */
    public void updateRequestedSimultaneousTaskCount(int[][] newRequestedSimultaneousTaskCount) {
        this.requestedSimultaneousTaskCount = newRequestedSimultaneousTaskCount;
    }

    /**
     * Dumps the request information into the given logger.
     * 
     * @param resourcesLogger Logger where to dump the information.
     * @param debug Whether the debug flag is enabled or not.
     */
    public void print(Logger resourcesLogger, boolean debug) {
        StringBuilder compositionString = new StringBuilder();
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : this.getRequested().getTypeComposition()
            .entrySet()) {
            compositionString.append(" \t\tTYPE = [\n").append("\t\t\tNAME = ").append(entry.getKey().getName())
                .append("\t\t\tCOUNT= ").append(entry.getValue()[0]).append("\t\t]\n");
        }

        resourcesLogger.info("ORDER_CREATION = [\n" + "\tTYPE_COMPOSITION = [" + compositionString.toString() + "]\n"
            + "\tPROVIDER = " + this.getProvider() + "\n" + "]");
        if (debug) {
            StringBuilder sb = new StringBuilder();
            sb.append("EXPECTED_SIM_TASKS = [").append("\n");
            for (int i = 0; i < CoreManager.getCoreCount(); i++) {
                for (int j = 0; j < this.requestedSimultaneousTaskCount()[i].length; ++j) {
                    sb.append("\t").append("IMPLEMENTATION_INFO = [").append("\n");
                    sb.append("\t\t").append("COREID = ").append(i).append("\n");
                    sb.append("\t\t").append("IMPLID = ").append(j).append("\n");
                    sb.append("\t\t").append("SIM_TASKS = ").append(this.requestedSimultaneousTaskCount()[i][j])
                        .append("\n");
                    sb.append("\t").append("]").append("\n");
                }
            }
            sb.append("]");
            resourcesLogger.debug(sb.toString());
        }
    }

}
