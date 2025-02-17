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
package es.bsc.compss.connectors.fake;

import es.bsc.compss.connectors.Connector;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.Cost;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ExtendedCloudMethodWorker;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class FakeConnector implements Connector, Cost {

    private static final Set<ResourceCreationRequest> PROCESSED_REQUESTS = new HashSet<>();


    public FakeConnector(CloudProvider provider, String connectorJarPath, String connectorMainClass,
        Map<String, String> connectorProperties) throws ConnectorException {
    }

    @Override
    public boolean isAutomaticScalingEnabled() {
        return true;
    }

    @Override
    public boolean turnON(String name, ResourceCreationRequest rR) {
        PROCESSED_REQUESTS.add(rR);
        return true;
    }

    public static Set<ResourceCreationRequest> getProcessedRequests() {
        return PROCESSED_REQUESTS;
    }

    @Override
    public void stopReached() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Long getNextCreationTime() throws ConnectorException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long getTimeSlot() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        if (worker instanceof CloudMethodWorker) {
            ((ExtendedCloudMethodWorker) worker).terminate();
        }
    }

    @Override
    public void terminateAll() {

    }

    @Override
    public Float getTotalCost() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Float currentCostPerHour() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Float getMachineCostPerHour(CloudMethodResourceDescription rc) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
