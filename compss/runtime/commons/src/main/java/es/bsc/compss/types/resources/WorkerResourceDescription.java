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
package es.bsc.compss.types.resources;

/**
 * Abstract representation of a Worker Resource.
 */
public abstract class WorkerResourceDescription extends ResourceDescription {

    // Unassigned values
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final int UNASSIGNED_INT = -1;
    public static final String UNASSIGNED_STR = "[unassigned]";
    public static final float UNASSIGNED_FLOAT = (float) -1.0;

    public static final int ZERO_INT = 0;
    public static final int ONE_INT = 1;


    /**
     * Empty worker resource.
     */
    public WorkerResourceDescription() {
        super();
    }

    /**
     * Worker resource constructed by copy.
     * 
     * @param desc Worker resource to be copied.
     */
    public WorkerResourceDescription(WorkerResourceDescription desc) {
        super(desc);
    }

    public abstract void scaleUpBy(int n);

    public abstract void scaleDownBy(int n);

}
