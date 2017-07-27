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
    public boolean turnON(String name, ResourceCreationRequest rR) {
        PROCESSED_REQUESTS.add(rR);
        return true;
    }

    public static Set<ResourceCreationRequest> getProcessedRequests() {
        return PROCESSED_REQUESTS;
    }

    @Override
    public void stopReached() {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public Long getNextCreationTime() throws ConnectorException {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public long getTimeSlot() {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
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
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public Float currentCostPerHour() {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

    @Override
    public Float getMachineCostPerHour(CloudMethodResourceDescription rc) {
        throw new UnsupportedOperationException("Not supported yet."); // To change body of generated methods, choose
                                                                       // Tools | Templates.
    }

}
