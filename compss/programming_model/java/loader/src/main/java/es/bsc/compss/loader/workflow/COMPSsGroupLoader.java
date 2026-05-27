/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.loader.workflow;

import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.api.Workflow;

public class COMPSsGroupLoader extends COMPSsGroup {

    private final Workflow wf;


    /**
     * Creates a new COMPSs group for the loader.
     * 
     * @param wf Workflow creating the group
     * @param groupName Group name.
     * @param implicitBarrier Whether to activate the implicit barrier or not.
     */
    public COMPSsGroupLoader(Workflow wf, String groupName, boolean implicitBarrier) {
        super(groupName, implicitBarrier);
        this.wf = wf;
        this.wf.openTaskGroup(this.groupName, implicitBarrier);
    }

    /**
     * Creates a new COMPSs group for the loader.
     *
     * @param wf Workflow creating the group
     * @param groupName Group name.
     */
    public COMPSsGroupLoader(Workflow wf, String groupName) {
        super(groupName);
        this.wf = wf;
        this.wf.openTaskGroup(this.groupName, true);
    }

    @Override
    public void close() throws Exception {
        this.wf.closeTaskGroup(this.groupName);
        if (this.barrier == true) {
            this.wf.barrierGroup(this.groupName);
        }
    }
}
