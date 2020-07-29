/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.data.LocationMonitor;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Resource;

import java.util.List;


public class ReadersInfo implements LocationMonitor {

    private Parameter parameter;
    private AbstractTask t;


    /**
     * Creates a new ReadersInfo instance with the given parameter {@code param} and TaskDescription {@code td}.
     * 
     * @param p Parameter.
     * @param task Task.
     */
    public ReadersInfo(Parameter p, AbstractTask task) {
        this.parameter = p;
        this.t = task;
    }

    /**
     * Returns the registered parameter.
     * 
     * @return The registered parameter.
     */
    public Parameter getParameter() {
        return this.parameter;
    }

    /**
     * Returns the registered TaskDescription.
     * 
     * @return The TaskDescription.
     */
    public AbstractTask getTask() {
        return this.t;
    }

    /**
     * Adds the location in the scheduling information of the actions.
     * 
     * @param resources Resources.
     * @param param Parameter to register.
     */
    public void addLocation(List<Resource> resources, Parameter param) {
        for (AllocatableAction aa : this.t.getExecutions()) {
            aa.getSchedulingInfo().setScore(resources, param);
        }
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[Parameter: ").append(this.parameter).append("]");

        buffer.append(", [Task description: ").append(this.t.toString()).append("]");

        buffer.append(")]");

        return buffer.toString();
    }

}
