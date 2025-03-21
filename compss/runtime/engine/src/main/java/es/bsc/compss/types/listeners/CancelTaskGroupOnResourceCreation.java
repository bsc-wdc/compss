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
package es.bsc.compss.types.listeners;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.ResourceManager;


/**
 * Listener to acknowledge the creation of resources through API calls.
 */
public class CancelTaskGroupOnResourceCreation implements ResourceCreationListener {

    private final AccessProcessor ap;
    private final Application application;

    private final int numTotalResources;
    private int grantedResources;

    private final String taskGroupName;


    /**
     * Creates a new ResourceCreationListener.
     * 
     * @param ap Associated AccessProcessor.
     * @param application Application.
     * @param numTotalResources Total number of resources to be requested.
     * @param taskGroupName Task group to be notified.
     */
    public CancelTaskGroupOnResourceCreation(AccessProcessor ap, Application application, int numTotalResources,
        String taskGroupName) {
        this.ap = ap;
        this.application = application;

        this.numTotalResources = numTotalResources;
        this.grantedResources = 0;
        this.taskGroupName = taskGroupName;
    }

    @Override
    public void notifyResourceCreation(ResourceDescription desc) {
        // Increase granted resources
        this.grantedResources = this.grantedResources + 1;

        // Check status
        if (this.grantedResources < this.numTotalResources) {
            // Keep requesting resources
            ResourceManager.requestResources(1, this);
        } else {
            // Petition totally fulfilled
            // Canceling associated task group if provided
            if (this.ap != null && this.application != null && this.taskGroupName != null
                && !this.taskGroupName.isEmpty() && !this.taskGroupName.equals("NULL")) {
                this.ap.cancelTaskGroup(this.application, this.taskGroupName);
            }
        }
    }

}
