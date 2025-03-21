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
package es.bsc.compss.types.resources.description;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.CoreManager;
import java.util.List;


public class CloudInstanceTypeDescription {

    private final String instanceTypeName;
    private final MethodResourceDescription rd;
    private int[] slotsCore;
    private int[][] slotsImpl;


    /**
     * Creates a new cloud instance type description.
     * 
     * @param name Instance type name.
     * @param rd Associated description.
     */
    public CloudInstanceTypeDescription(String name, MethodResourceDescription rd) {
        this.instanceTypeName = name;
        this.rd = rd;

        int coreCount = CoreManager.getCoreCount();
        this.slotsCore = new int[coreCount];
        this.slotsImpl = new int[coreCount][];

        // Get new values
        for (int coreId = 0; coreId < coreCount; coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            int implsSize = impls.size();
            this.slotsImpl[coreId] = new int[implsSize];
            for (int implId = 0; implId < implsSize; ++implId) {
                Implementation impl = impls.get(implId);
                if (impl.getTaskType() == TaskType.METHOD) {
                    MethodResourceDescription reqs = (MethodResourceDescription) impl.getRequirements();
                    Integer into = rd.canHostSimultaneously(reqs);
                    this.slotsCore[coreId] = Math.max(this.slotsCore[coreId], into);
                    this.slotsImpl[coreId][implId] = into;
                }
            }
        }
    }

    /**
     * Returns the instance type name.
     * 
     * @return The instance type name.
     */
    public String getName() {
        return this.instanceTypeName;
    }

    /**
     * Returns the instance type description.
     * 
     * @return The instance type description.
     */
    public MethodResourceDescription getResourceDescription() {
        return this.rd;
    }

    /**
     * Returns the slots per core that can run on the current instance type.
     * 
     * @return The slots per core that can run on the current instance type.
     */
    public int[] getSlotsCore() {
        return this.slotsCore;
    }

    /**
     * Returns the slots of the core with index {@code index} that can run on the current instance type.
     * 
     * @param index Core index.
     * @return The slots of the core with index {@code index} that can run on the current instance type.
     */
    public int getSpecificSlotsCore(int index) {
        return this.slotsCore[index];
    }

    /**
     * Sets a new number of slots per core.
     * 
     * @param slotsCore New slots per core.
     */
    public void setSlotsCore(int[] slotsCore) {
        this.slotsCore = slotsCore;
    }

    /**
     * Returns the slots per implementation per core that can run on the current instance type.
     * 
     * @return The slots per implementation per core that can run on the current instance type.
     */
    public int[][] getSlotsImpl() {
        return this.slotsImpl;
    }

    /**
     * Returns the length of the slots per implementation (number of cores).
     * 
     * @return The length of the slots per implementation (number of cores).
     */
    public int getSlotsImplLength() {
        return this.slotsImpl.length;
    }

    /**
     * Returns the slots per implementation of the core with the given index {@code coreId}.
     * 
     * @param coreId Core Index.
     * @return The slots per implementation of the core with the given index {@code coreId}.
     */
    public int[] getSpecificSlotsImpl(int coreId) {
        return this.slotsImpl[coreId];
    }

    /**
     * Returns the slots of the implementation with index {@code implId} and core with index {@code coreId}.
     * 
     * @param coreId Core index.
     * @param implId Implementation index.
     * @return The slots of the implementation with index {@code implId} and core with index {@code coreId}.
     */
    public int getSpecificSlotsImpl(int coreId, int implId) {
        return this.slotsImpl[coreId][implId];
    }

    /**
     * Sets a new slots per implementation per core.
     * 
     * @param slotsImpl New slots per implementation per core.
     */
    public void setSlotsImpl(int[][] slotsImpl) {
        this.slotsImpl = slotsImpl;
    }

}
