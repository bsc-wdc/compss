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
package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
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

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CM_COMP);

    // Relation between the name of an template and its features.
    private final HashMap<String, CloudInstanceTypeDescription> types;


    /**
     * Constructs a new CloudImageManager.
     */
    public CloudTypeManager() {
        LOGGER.debug("Initializing CloudTypeManager");
        this.types = new HashMap<>();
    }

    /**
     * Adds a new instance type which can be used by the Cloud Provider.
     *
     * @param type Description of the resource.
     */
    public void addType(CloudInstanceTypeDescription type) {
        LOGGER.debug("Add new type description " + type.getName());
        this.types.put(type.getName(), type);
    }

    /**
     * Finds all the types provided by the Cloud Provider which fulfill the resource description.
     *
     * @param requested description of the features that the image must provide.
     * @return The best instance type provided by the Cloud Provider which fulfills the resource description.
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
     * Return all the instance type names offered by that Cloud Provider.
     *
     * @return set of instance type names offered by that Cloud Provider.
     */
    public Set<String> getAllTypeNames() {
        return this.types.keySet();
    }

    /**
     * Return all the instance types offered by that Cloud Provider.
     *
     * @return set of instance types offered by that Cloud Provider.
     */
    public Collection<CloudInstanceTypeDescription> getAllTypes() {
        return this.types.values();
    }

    /**
     * Returns the type of the instance.
     * 
     * @param name Instance name.
     * @return Instance type description associated to that name.
     */
    public CloudInstanceTypeDescription getType(String name) {
        return this.types.get(name);
    }

    /**
     * Returns the simultaneous implementations that a given type can run.
     * 
     * @param type Instance type.
     * @return Simultaneous implementations per each implementation of each coreElement.
     */
    public int[][] getSimultaneousImpls(String type) {
        CloudInstanceTypeDescription t = this.types.get(type);
        if (t != null) {
            return t.getSlotsImpl();
        }
        return null;
    }

    /**
     * Updates the coreElements information.
     * 
     * @param newCores New coreElements.
     */
    public void newCoreElementsDetected(List<Integer> newCores) {
        int coreCount = CoreManager.getCoreCount();
        for (CloudInstanceTypeDescription type : this.types.values()) {
            int[][] slotsI = new int[coreCount][];
            // Copy actual values
            int[] slotsC = Arrays.copyOf(type.getSlotsCore(), coreCount);
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

    /**
     * Dumps the current state.
     * 
     * @param prefix Prefix to append to the dump.
     * @return String representing the dump of the current state.
     */
    public String getCurrentState(String prefix) {
        int coreCount = CoreManager.getCoreCount();
        StringBuilder sb = new StringBuilder();
        // Types
        sb.append(prefix).append("TYPES = [").append("\n");
        for (java.util.Map.Entry<String, CloudInstanceTypeDescription> type : this.types.entrySet()) {
            sb.append(prefix).append("\t").append("TYPE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("KEY = ").append(type.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("CORES = [").append("\n");
            for (int i = 0; i < coreCount; i++) {
                sb.append(prefix).append("\t").append("\t").append("\t").append("CORE = [").append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i)
                    .append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("SLOTS = ")
                    .append(type.getValue().getSpecificSlotsCore(i)).append("\n");
                sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
            }
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");

            sb.append(prefix).append("\t").append("\t").append("IMPLEMENTATIONS = [").append("\n");
            for (int i = 0; i < coreCount; ++i) {
                for (int j = 0; j < CoreManager.getNumberCoreImplementations(i); ++j) {
                    sb.append(prefix).append("\t").append("\t").append("\t").append("IMPLEMENTATION = [").append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("COREID = ").append(i)
                        .append("\n");
                    sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("IMPLID = ").append(j)
                        .append("\n");
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
