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
package es.bsc.compss.loader.total;

import es.bsc.compss.api.COMPSsGroup;
import es.bsc.compss.api.COMPSsRuntime;


public class COMPSsGroupLoader extends COMPSsGroup {

    private final COMPSsRuntime api;
    private long appId;


    /**
     * Creates a new COMPSs group for the loader.
     * 
     * @param api COMPSs Runtime API.
     * @param appId Application Id.
     * @param groupName Group name.
     * @param implicitBarrier Whether to activate the implicit barrier or not.
     */
    public COMPSsGroupLoader(COMPSsRuntime api, Long appId, String groupName, boolean implicitBarrier) {
        super(groupName, implicitBarrier);

        this.api = api;
        this.appId = appId;
        this.api.openTaskGroup(this.groupName, implicitBarrier, this.appId);

    }

    /**
     * Creates a new COMPSs group for the loader.
     * 
     * @param api COMPSs Runtime API.
     * @param appId Application Id.
     * @param groupName Group name.
     */
    public COMPSsGroupLoader(COMPSsRuntime api, Long appId, String groupName) {
        super(groupName);

        this.api = api;
        this.appId = appId;
        this.api.openTaskGroup(this.groupName, true, this.appId);

    }

    @Override
    public void close() throws Exception {
        this.api.closeTaskGroup(this.groupName, this.appId);
        if (this.barrier == true) {
            this.api.barrierGroup(appId, this.groupName);
        }
    }
}
