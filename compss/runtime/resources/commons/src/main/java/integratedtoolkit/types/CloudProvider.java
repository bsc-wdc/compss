package integratedtoolkit.types;

import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.ITConstants;
import integratedtoolkit.connectors.Connector;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.Cost;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.CloudImageManager;
import integratedtoolkit.util.CloudTypeManager;
import integratedtoolkit.util.CoreManager;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import integratedtoolkit.log.Loggers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CloudProvider {

    private final String name;
    private final Integer limitOfVMs;

    private final CloudImageManager imgManager;
    private final CloudTypeManager typeManager;

    private final Connector connector;
    private final Cost cost;

    private int currentVMCount;

    // Loggers
    private static final Logger logger = LogManager.getLogger(Loggers.CM_COMP);
    private static final String WARN_NO_COMPATIBLE_TYPE = "WARN: Cannot find any compatible instanceType";
    private static final String WARN_NO_COMPATIBLE_IMAGE = "WARN: Cannot find any compatible Image";
    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";

    public CloudProvider(String providerName, Integer limitOfVMs, String connectorJarPath, String connectorMainClass,
            HashMap<String, String> connectorProperties)
            throws ConnectorException {

        this.name = providerName;
        this.limitOfVMs = limitOfVMs;
        this.currentVMCount = 0;

        this.imgManager = new CloudImageManager();
        this.typeManager = new CloudTypeManager();

        // Load Runtime connector implementation that will finally load the
        // infrastructure dependent connector
        try {
            Class<?> conClass = Class.forName(System.getProperty(ITConstants.IT_CONN));
            Class<?>[] parameterTypes = new Class<?>[]{String.class, String.class, String.class, HashMap.class};
            Constructor<?> ctor = conClass.getConstructor(parameterTypes);
            Object conector = ctor.newInstance(providerName, connectorJarPath, connectorMainClass, connectorProperties);
            connector = (Connector) conector;
            cost = (Cost) conector;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /*
     * ---------------------------------------- 
     * ------- Cloud Provider Builders --------
     * ----------------------------------------
     */
    public void addCloudImage(CloudImageDescription cid) {
        imgManager.add(cid);
    }

    public void addInstanceType(CloudMethodResourceDescription rd) {
        typeManager.add(rd);
    }

    public void newCoreElementsDetected(List<Integer> newCores) {
        typeManager.newCoreElementsDetected(newCores);
    }

    /*
     * ----------------------------------------- 
     * ------------- Basic Queries -------------
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

    public Set<String> getAllImageNames() {
        return imgManager.getAllImageNames();
    }

    public CloudImageDescription getImage(String name) {
        return imgManager.getImage(name);
    }

    public Set<String> getAllInstanceTypeNames() {
        return typeManager.getAllInstanceTypeNames();
    }

    public CloudMethodResourceDescription getInstanceType(String name) {
        return typeManager.getInstanceType(name);
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

    public HashMap<CloudMethodResourceDescription, Integer> getVMComposition(String name) {
        return typeManager.getComposition(name);
    }

    /*
     * ----------------------------------------- 
     * ------------- State Changes -------------
     * -----------------------------------------
     */
    public void stopReached() {
        connector.stopReached();
    }

    public boolean turnON(ResourceCreationRequest rcr) {
        currentVMCount++;
        return connector.turnON("compss" + UUID.randomUUID().toString(), rcr);
    }

    public void createdVM(String resourceName, CloudMethodResourceDescription rd) {
        typeManager.createdVM(resourceName, rd.getType());
    }

    public void refusedWorker(CloudMethodResourceDescription rd) {
        currentVMCount--;
    }

    public void turnOff(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        typeManager.reduceVM(name, reduction.getType());
        connector.terminate(worker, reduction);
        currentVMCount--;
    }

    public int getCurrentVMCount() {
        return currentVMCount;
    }

    /*
     * ------------------------------------------ 
     * -------- Recommendation Queries ----------
     * ------------------------------------------
     */
    public CloudMethodResourceDescription getBestIncrease(Integer amount, MethodResourceDescription constraints, boolean contained) {
        // Check Cloud capabilities
        if (limitOfVMs != null && limitOfVMs != -1 && currentVMCount >= limitOfVMs) {
            return null;
        }

        // Select all the compatible types
        LinkedList<CloudMethodResourceDescription> instances = typeManager.getCompatibleTypes(constraints);
        if (instances.isEmpty()) {
            logger.warn(WARN_NO_COMPATIBLE_TYPE);
            return null;
        }

        CloudMethodResourceDescription result = null;
        if (contained) {
            result = selectContainedInstance(instances, constraints, amount);
        } else {
            result = selectContainingInstance(instances, constraints, amount);
        }

        // Pick an image to be loaded in the Type (or return null)
        if (result != null) {
            // Select all the compatible images
            LinkedList<CloudImageDescription> images = imgManager.getCompatibleImages(constraints);
            if (images.isEmpty()) {
                logger.warn(WARN_NO_COMPATIBLE_IMAGE);
                return null;
            }
            result.setProviderName(images.get(0).getProviderName());
            result.setImage(images.get(0));
            result.setValue(cost.getMachineCostPerHour(result));
        } else {
            logger.warn(WARN_NO_VALID_INSTANCE);
        }

        return result;
    }

    public CloudMethodResourceDescription getResourceDescription(String instanceTypeName, String imageName) {
        CloudMethodResourceDescription result = typeManager.getInstanceType(instanceTypeName);
        if (result != null) {
            CloudImageDescription image = imgManager.getImage(imageName);
            result.setProviderName(name);
            result.setImage(image);
            result.setValue(cost.getMachineCostPerHour(result));
        } else {
            logger.warn(WARN_NO_VALID_INSTANCE);
        }
        return result;
    }

    private CloudMethodResourceDescription selectContainingInstance(LinkedList<CloudMethodResourceDescription> instances,
            MethodResourceDescription constraints, int amount) {

        CloudMethodResourceDescription result = null;
        float bestDistance = Integer.MIN_VALUE;

        for (CloudMethodResourceDescription rd : instances) {
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            logger.debug(
                    "Can host: slots = " + slots + " amount = " + amount + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance > 0.0) {
                continue;
            }

            if (distance > bestDistance) {
                result = rd;
                bestDistance = distance;
            } else if (distance == bestDistance && result != null) {
                if (result.getValue() != null && rd.getValue() != null && result.getValue() > rd.getValue()) {
                    // Evaluate optimal candidate
                    result = rd;
                    bestDistance = distance;
                }
            }
        }

        if (result == null) {
            return null;
        }
        return new CloudMethodResourceDescription(result);
    }

    private CloudMethodResourceDescription selectContainedInstance(LinkedList<CloudMethodResourceDescription> instances,
            MethodResourceDescription constraints, int amount) {

        CloudMethodResourceDescription result = null;
        float bestDistance = Integer.MAX_VALUE;

        for (CloudMethodResourceDescription rd : instances) {
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            logger.debug(
                    "Can host: slots = " + slots + " amount = " + amount + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance < 0.0) {
                continue;
            }

            if (distance < bestDistance) {
                result = rd;
                bestDistance = distance;
            } else if (distance == bestDistance && result != null) {
                if (result.getValue() != null && rd.getValue() != null && result.getValue() > rd.getValue()) {
                    // Evaluate optimal candidate
                    result = rd;
                    bestDistance = distance;
                }
            }

        }

        if (result == null) {
            return null;
        }
        return new CloudMethodResourceDescription(result);
    }

    // TypeName -> [[# modified CE that weren't requested,
    // #slots removed that weren't requested,
    // #slots removed that were requested],
    // Type description]
    public HashMap<String, Object[]> getPossibleReductions(CloudMethodWorker res, float[] recommendedSlots) {
        HashMap<String, Object[]> reductions = new HashMap<>();
        HashMap<String, Object[]> types = typeManager.getPossibleReductions(res.getName());

        for (java.util.Map.Entry<String, Object[]> type : types.entrySet()) {
            String typeName = type.getKey();
            Object[] description = type.getValue();
            int[] reducedSlots = (int[]) description[0];
            float[] values = new float[3];
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (res.canRun(coreId)) {
                    if (recommendedSlots[coreId] < 1 && reducedSlots[coreId] > 0) {
                        values[0]++; // Adding a desired CE whose slots will be
                        // destroyed
                        values[1] += reducedSlots[coreId]; // all reduced slots
                        // weren't requested
                    } else {
                        float dif = (float) reducedSlots[coreId] - recommendedSlots[coreId];
                        if (dif < 0) {
                            values[2] += reducedSlots[coreId];
                        } else {
                            values[2] += recommendedSlots[coreId];
                            values[1] += dif;
                        }
                    }
                }
            }
            description[0] = values;
            reductions.put(typeName, description);
        }
        return reductions;
    }

    /*
     * ----------------------------------------- 
     * ------------- Debug Queries -------------
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
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

    public CloudImageManager getCloudImageManager() {
        return imgManager;
    }

    public CloudTypeManager getCloudTypeManager() {
        return typeManager;
    }

    public void terminateAll() {
        this.currentVMCount = 0;
        typeManager.clearAll();
        connector.terminateAll();
    }

}
