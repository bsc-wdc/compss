package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CloudTypeManager {

    /**
     * Relation between the name of an template and its features
     */
    private final HashMap<String, CloudInstanceTypeDescription> types;

    private static final Logger logger = LogManager.getLogger(Loggers.CM_COMP);


    /**
     * Constructs a new CloudImageManager
     */
    public CloudTypeManager() {
        logger.debug("Initializing CloudTypeManager");
        types = new HashMap<>();
    }

    /**
     * Adds a new instance type which can be used by the Cloud Provider
     *
     * @param type
     *            Description of the resource
     */
    public void addType(CloudInstanceTypeDescription type) {
        logger.debug("Add new type description " + type.getName());
        types.put(type.getName(), type);
    }

    /**
     * Finds all the types provided by the Cloud Provider which fulfill the resource description.
     *
     * @param requested
     *            description of the features that the image must provide
     * @return The best instance type provided by the Cloud Provider which fulfills the resource description
     */
    public List<CloudInstanceTypeDescription> getCompatibleTypes(MethodResourceDescription requested) {
        List<CloudInstanceTypeDescription> compatiblesList = new LinkedList<>();
        if (!this.types.isEmpty()) {
            for (CloudInstanceTypeDescription type : this.types.values()) {
                MethodResourceDescription resources = type.getResourceDescription();
                if (resources.contains(requested)) {
                    // Satisfies the constraints, add compatible
                    compatiblesList.add(type);
                }
            }
        } else {
            CloudInstanceTypeDescription citd = new CloudInstanceTypeDescription("NO TYPE", requested);
            compatiblesList.add(citd);
        }
        return compatiblesList;
    }

    /**
     * Return all the instance type names offered by that Cloud Provider
     *
     * @return set of instance type names offered by that Cloud Provider
     */
    public Set<String> getAllTypeNames() {
        return types.keySet();
    }

    /**
     * Return all the instance types offered by that Cloud Provider
     *
     * @return set of instance types offered by that Cloud Provider
     */
    public Collection<CloudInstanceTypeDescription> getAllTypes() {
        return types.values();
    }

    /**
     *
     * @param name
     * @return instance type description associated to that name
     */
    public CloudInstanceTypeDescription getType(String name) {
        return types.get(name);
    }

    public int[][] getSimultaneousImpls(String type) {
        CloudInstanceTypeDescription t = types.get(type);
        if (t != null) {
            return t.getSlotsImpl();
        }
        return null;
    }

    public void newCoreElementsDetected(List<Integer> newCores) {
        int coreCount = CoreManager.getCoreCount();
        for (CloudInstanceTypeDescription type : types.values()) {
            int[][] slotsI = new int[coreCount][];
            // Copy actual values
            int[] slotsC = Arrays.copyOf(type.getSlotsCore(),coreCount);
            for (int i = 0; i < type.getSlotsImplLength(); ++i) {
                int[] slotsImpl = type.getSpecificSlotsImpl(i);
                slotsI[i] = slotsImpl.clone();
            }
            // Get new values
            for (int coreId : newCores) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                int implsSize = impls.size();
                slotsI[coreId] = new int[implsSize];
                for (int implId = 0; implId < implsSize; ++implId) {
                    Implementation impl = impls.get(implId);
                    if (impl.getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription rd = (MethodResourceDescription) impl.getRequirements();
                        Integer into = type.getResourceDescription().canHostSimultaneously(rd);
                        slotsC[coreId] = Math.max(slotsC[coreId], into);
                        slotsI[coreId][implId] = into;
                    }
                }
            }
            type.setSlotsCore(slotsC);
            type.setSlotsImpl(slotsI);
        }
    }

    public String getCurrentState(String prefix) {
        int coreCount = CoreManager.getCoreCount();
        StringBuilder sb = new StringBuilder();
        // Types
        sb.append(prefix).append("TYPES = [").append("\n");
        for (java.util.Map.Entry<String, CloudInstanceTypeDescription> type : types.entrySet()) {
            sb.append(prefix).append("\t").append("TYPE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("KEY = ").append(type.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("CORES = [").append("\n");
            for (int i = 0; i < coreCount; i++) {
                sb.append(prefix).append("\t").append("\t").append("\t").append("CORE = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SLOTS = ")
                        .append(type.getValue().getSpecificSlotsCore(i)).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");

            sb.append(prefix).append("\t").append("\t").append("IMPLEMENTATIONS = [").append("\n");
            for (int i = 0; i < coreCount; ++i) {
                for (int j = 0; j < CoreManager.getNumberCoreImplementations(i); ++j) {
                    sb.append(prefix).append("\t").append("\t").append("\t").append("IMPLEMENTATION = [").append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("IMPLID = ").append(j).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SLOTS = ")
                            .append(type.getValue().getSpecificSlotsImpl(i, j)).append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
                }
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");

            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

}
