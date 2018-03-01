package es.bsc.compss.types;

import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.connectors.Connector;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.Cost;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.CloudImageManager;
import es.bsc.compss.util.CloudTypeManager;
import es.bsc.compss.util.CoreManager;

import java.lang.reflect.Constructor;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import java.util.Collection;
import java.util.HashSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CloudProvider {

    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_NO_VALID_IMAGE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_CANNOT_TURN_ON = "WARN: Connector cannot turn on resource";

    private final String name;
    private final Integer limitOfVMs;

    private final HashSet<CloudMethodWorker> hostedWorkers;
    private final CloudImageManager imgManager;
    private final CloudTypeManager typeManager;

    private final Connector connector;
    private final Cost cost;

    private int currentVMCount;
    private final List<ResourceCreationRequest> pendingRequests;
    private int[] pendingCoreCount;

    // Loggers
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CM_COMP);


    public CloudProvider(String providerName, Integer limitOfVMs, String runtimeConnectorClass, String connectorJarPath,
            String connectorMainClass, Map<String, String> connectorProperties) throws ConnectorException {

        this.name = providerName;
        this.limitOfVMs = limitOfVMs;
        this.currentVMCount = 0;
        this.hostedWorkers = new HashSet<>();
        this.imgManager = new CloudImageManager();
        this.typeManager = new CloudTypeManager();

        // Load Runtime connector implementation that will finally load the
        // infrastructure dependent connector
        try {
            Class<?> conClass = Class.forName(runtimeConnectorClass);
            Class<?>[] parameterTypes = new Class<?>[] { CloudProvider.class, String.class, String.class, Map.class };
            Constructor<?> ctor = conClass.getConstructor(parameterTypes);
            Object conector = ctor.newInstance(this, connectorJarPath, connectorMainClass, connectorProperties);
            connector = (Connector) conector;
            cost = (Cost) conector;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
        pendingRequests = new LinkedList<>();
        pendingCoreCount = new int[CoreManager.getCoreCount()];
    }

    /*
     * ---------------------------------------- ------- Cloud Provider Builders --------
     * ----------------------------------------
     */
    /**
     * Adds an image description to the Cloud Provider
     *
     * @param cid
     *            Description of the features offered by that image
     */
    public void addCloudImage(CloudImageDescription cid) {
        imgManager.add(cid);
    }

    /**
     * Adds an instance type description to a Cloud Provider
     *
     * @param rd
     *            Description of the features offered by that instance type
     *
     */
    public void addInstanceType(CloudInstanceTypeDescription rd) {
        typeManager.addType(rd);
    }

    public void newCoreElementsDetected(List<Integer> newCores) {
        typeManager.newCoreElementsDetected(newCores);
        this.pendingCoreCount = new int[CoreManager.getCoreCount()];
        for (ResourceCreationRequest creationRequest : pendingRequests) {
            int[][] newRequestedSimultaneousTaskCount = this.computeSimultaneousCounts(creationRequest.getRequested());
            creationRequest.updateRequestedSimultaneousTaskCount(newRequestedSimultaneousTaskCount);

            for (int coreId = 0; coreId < newRequestedSimultaneousTaskCount.length; coreId++) {
                int coreSlots = 0;
                for (int implId = 0; implId < newRequestedSimultaneousTaskCount[coreId].length; implId++) {
                    coreSlots = Math.max(coreSlots, newRequestedSimultaneousTaskCount[coreId][implId]);
                }
                this.pendingCoreCount[coreId] += coreSlots;
            }
        }
    }

    /*
     * ----------------------------------------- ------------- Basic Queries -------------
     * ----------------------------------------
     */
    public String getName() {
        return name;
    }

    public float getCurrentCostPerHour() {
        return cost.currentCostPerHour();
    }

    public float getTotalCost() {
        return cost.getTotalCost();
    }

    public Collection<CloudInstanceTypeDescription> getAllTypes() {
        return typeManager.getAllTypes();
    }

    public Set<String> getAllInstanceTypeNames() {
        return typeManager.getAllTypeNames();
    }

    public CloudInstanceTypeDescription getInstanceType(String name) {
        return typeManager.getType(name);
    }

    public List<CloudInstanceTypeDescription> getCompatibleTypes(MethodResourceDescription requirements) {
        return typeManager.getCompatibleTypes(requirements);
    }

    public Collection<CloudImageDescription> getAllImages() {
        return imgManager.getAllImages();
    }

    public Set<String> getAllImageNames() {
        return imgManager.getAllImageNames();
    }

    public CloudImageDescription getImage(String name) {
        return imgManager.getImage(name);
    }

    public List<CloudImageDescription> getCompatibleImages(MethodResourceDescription requirements) {
        return imgManager.getCompatibleImages(requirements);
    }

    public Float getInstanceCostPerHour(CloudMethodResourceDescription instanceDescription) {
        return cost.getMachineCostPerHour(instanceDescription);
    }

    public int[][] getSimultaneousImpls(String type) {
        return typeManager.getSimultaneousImpls(type);
    }

    public long getNextCreationTime() throws Exception {
        return connector.getNextCreationTime();
    }

    public long getTimeSlot() throws Exception {
        return connector.getTimeSlot();
    }

    /*
     * ----------------------------------------- ------------- State Changes -------------
     * -----------------------------------------
     */
    public void stopReached() {
        connector.stopReached();
    }

    public ResourceCreationRequest requestResourceCreation(CloudMethodResourceDescription instanceDescription) {
        int[][] simultaneousCounts = computeSimultaneousCounts(instanceDescription);
        ResourceCreationRequest rcr = new ResourceCreationRequest(instanceDescription, simultaneousCounts, this);
        LOGGER.debug("[Cloud Manager] Asking for resource creation " + instanceDescription.getName() + " with image "
                + instanceDescription.getImage().getImageName());
        boolean isRequestAccepted = connector.turnON("compss" + UUID.randomUUID().toString(), rcr);
        if (isRequestAccepted) {
            CloudMethodResourceDescription cmrd = rcr.getRequested();
            for (int[] typeCount : cmrd.getTypeComposition().values()) {
                currentVMCount += typeCount[0];
            }
            pendingRequests.add(rcr);
            for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
                int coreSlots = 0;
                for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                    coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
                }
                this.pendingCoreCount[coreId] += coreSlots;
            }
            return rcr;
        } else {
            LOGGER.warn(WARN_CANNOT_TURN_ON);
            return null;
        }
    }

    private int[][] computeSimultaneousCounts(CloudMethodResourceDescription cloudDescription) {
        int coreCount = CoreManager.getCoreCount();
        int[][] simultaneousCounts = new int[coreCount][];

        for (int coreId = 0; coreId < coreCount; coreId++) {
            int implCount = CoreManager.getNumberCoreImplementations(coreId);
            simultaneousCounts[coreId] = new int[implCount];
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> typeEntry : cloudDescription.getTypeComposition().entrySet()) {
            CloudInstanceTypeDescription citd = typeEntry.getKey();
            int count = typeEntry.getValue()[0];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                    simultaneousCounts[coreId][implId] += citd.getSlotsImpl()[coreId][implId] * count;
                }
            }
        }

        return simultaneousCounts;
    }

    public void refusedCreation(ResourceCreationRequest rcr) {
        CloudMethodResourceDescription cmrd = rcr.getRequested();
        for (int[] typeCount : cmrd.getTypeComposition().values()) {
            currentVMCount -= typeCount[0];
        }
        pendingRequests.remove(rcr);
        int[][] simultaneousCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
            }
            this.pendingCoreCount[coreId] -= coreSlots;
        }
    }

    public void confirmedCreation(ResourceCreationRequest rcr, CloudMethodWorker worker, CloudMethodResourceDescription granted) {
        CloudMethodResourceDescription cmrd = rcr.getRequested();
        for (int[] typeCount : cmrd.getTypeComposition().values()) {
            currentVMCount -= typeCount[0];
        }
        for (int[] typeCount : granted.getTypeComposition().values()) {
            currentVMCount += typeCount[0];
        }
        pendingRequests.remove(rcr);
        int[][] simultaneousCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
            }
            this.pendingCoreCount[coreId] -= coreSlots;
        }
        hostedWorkers.add(worker);
    }

    public List<ResourceCreationRequest> getPendingRequests() {
        return pendingRequests;
    }

    public int[] getPendingCoreCounts() {
        return this.pendingCoreCount;
    }

    public void requestResourceReduction(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        LOGGER.debug("[Cloud Manager] Destroying resource " + worker.getName() + " for reduction");
        Map<CloudInstanceTypeDescription, int[]> composition = reduction.getTypeComposition();
        for (int[] typeCount : composition.values()) {
            currentVMCount -= typeCount[0];
        }
        if (worker.getDescription().getTypeComposition().isEmpty()) {
            hostedWorkers.remove(worker);
        }
        connector.terminate(worker, reduction);
    }

    public int getCurrentVMCount() {
        return currentVMCount;
    }

    public Set<CloudMethodWorker> getHostedWorkers() {
        return hostedWorkers;
    }
    
    public CloudMethodWorker getHostedWorker(String name) {
    	for (CloudMethodWorker vm : hostedWorkers) {
    		if (vm.getName().equals(name)){
    			return vm;
    		}    
        }
    	return null;
    }

    /*
     * ------------------------------------------ ------- Recommendation Queries ----------
     * ------------------------------------------
     */
    public boolean canHostMoreInstances() {
        if (limitOfVMs == null) {
            return true;
        }
        if (limitOfVMs == -1) {
            return true;
        }
        return currentVMCount < limitOfVMs;
    }

    public CloudMethodResourceDescription getResourceDescription(String instanceTypeName, String imageName) {
        CloudMethodResourceDescription result = null;
        CloudInstanceTypeDescription type = typeManager.getType(instanceTypeName);
        if (type != null) {
            CloudImageDescription image = imgManager.getImage(imageName);
            if (image != null) {
                result = new CloudMethodResourceDescription(type, image);
                result.setValue(cost.getMachineCostPerHour(result));
            } else {
                LOGGER.warn(WARN_NO_VALID_IMAGE);
            }
        } else {
            LOGGER.warn(WARN_NO_VALID_INSTANCE);
        }
        return result;
    }

    /*
     * ----------------------------------------- ------------- Debug Queries -------------
     * ----------------------------------------
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("PROVIDER = [").append("\n");
        sb.append(prefix).append("\t").append("NAME = ").append(name).append("\n");
        sb.append(prefix).append("\t").append("CURRENT_VM = ").append(currentVMCount).append("\n");
        sb.append(prefix).append("\t").append("LIMIT_VM = ").append(limitOfVMs).append("\n");
        sb.append(imgManager.getCurrentState(prefix + "\t"));
        sb.append(typeManager.getCurrentState(prefix + "\t"));

        // Virtual Instances
        sb.append(prefix).append("\t").append("VIRTUAL_INSTANCES = [").append("\n");
        for (CloudMethodWorker vm : hostedWorkers) {
            CloudMethodResourceDescription cmrd = vm.getDescription();
            sb.append(cmrd.getCurrentState(prefix + "\t")).append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

    public void terminateAll() {
        this.currentVMCount = 0;
        hostedWorkers.clear();
        connector.terminateAll();
    }
    
 
    
}
