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
package es.bsc.compss.connectors;

import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Cost {

    /**
     * Returns the total instance cost.
     * 
     * @return The total instance cost.
     */
    public Float getTotalCost();

    /**
     * Returns the cost per hour.
     * 
     * @return The cost per hour.
     */
    public Float currentCostPerHour();

    /**
     * Returns the cost per hour for a set of machines.
     * 
     * @param rc Resource description.
     * @return The cost per hour for a set of machines.
     */
    public Float getMachineCostPerHour(CloudMethodResourceDescription rc);

}
