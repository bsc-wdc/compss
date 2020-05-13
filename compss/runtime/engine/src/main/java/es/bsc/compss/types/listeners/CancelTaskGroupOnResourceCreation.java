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
package es.bsc.compss.types.listeners;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.types.resources.ResourceDescription;


/**
 * Listener to acknowledge the creation of resources through API calls.
 */
public class CancelTaskGroupOnResourceCreation implements ResourceCreationListener {

    private final AccessProcessor taskProducer;
    private final Long appId;
    private final String groupName;


    /**
     * Creates a new ResourceCreationListener.
     * 
     * @param taskProducer Associated task producer.
     * @param appId Application Id.
     * @param groupName Task group to be notified.
     */
    public CancelTaskGroupOnResourceCreation(AccessProcessor taskProducer, Long appId, String groupName) {
        this.taskProducer = taskProducer;
        this.appId = appId;
        this.groupName = groupName;
    }

    @Override
    public void notifyResourceCreation(ResourceDescription desc) {
        // Cancel associated task group (if provided)
        if (this.taskProducer != null && this.appId != null && this.groupName != null && !this.groupName.isEmpty()
            && !this.groupName.equals("NULL")) {
            this.taskProducer.cancelTaskGroup(this.appId, this.groupName);
        }

    }

}
