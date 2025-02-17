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

import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.util.ResourceManager;

import java.util.Map;


public class CloudMethodWorker extends DynamicMethodWorker {

    private final CloudProvider provider;


    /**
     * Creates a new CloudMethodWorker instance.
     * 
     * @param name Worker name.
     * @param provider Associated Cloud Provider.
     * @param description Worker description.
     * @param worker COMPSs worker.
     * @param limitOfTasks Limit of CPU tasks.
     * @param limitGPUTasks Limit of GPU tasks.
     * @param limitFPGATasks Limit of FPGA tasks.
     * @param limitOTHERTasks Limit of OTHER tasks.
     * @param sharedDisks Mounted shared disks.
     */
    public CloudMethodWorker(String name, CloudProvider provider, CloudMethodResourceDescription description,
        COMPSsWorker worker, int limitOfTasks, int limitGPUTasks, int limitFPGATasks, int limitOTHERTasks,
        Map<String, String> sharedDisks) {

        super(name, description, worker, limitOfTasks, limitGPUTasks, limitFPGATasks, limitOTHERTasks, sharedDisks);
        this.provider = provider;
    }

    /**
     * Creates a new CloudMethodWorker instance.
     * 
     * @param name Worker name.
     * @param provider Associated Cloud Provider.
     * @param description Worker description.
     * @param config Worker configuration.
     * @param sharedDisks Mounted shared disks.
     */
    public CloudMethodWorker(String name, CloudProvider provider, CloudMethodResourceDescription description,
        MethodConfiguration config, Map<String, String> sharedDisks) {

        super(name, description, config, sharedDisks);
        this.provider = provider;

        if (this.description != null) {
            // Add name
            ((CloudMethodResourceDescription) this.description).setName(name);
        }

    }

    /**
     * Clones the given CloudMethodWorker.
     * 
     * @param cmw CloudMethodWorker to clone.
     */
    public CloudMethodWorker(CloudMethodWorker cmw) {
        super(cmw);
        this.provider = cmw.provider;
    }

    /**
     * Returns the associated provider.
     * 
     * @return The associated provider.
     */
    public CloudProvider getProvider() {
        return this.provider;
    }

    @Override
    public CloudMethodResourceDescription getDescription() {
        return (CloudMethodResourceDescription) super.getDescription();
    }

    @Override
    public String getMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(super.getMonitoringData(prefix));

        String providerName = this.provider.getName();
        if (providerName == null) {
            providerName = "";
        }
        sb.append(prefix).append("<Provider>").append(providerName).append("</Provider>").append("\n");

        CloudImageDescription image = ((CloudMethodResourceDescription) this.description).getImage();
        String imageName = "";
        if (image != null) {
            imageName = image.getImageName();
        }
        sb.append(prefix).append("<Image>").append(imageName).append("</Image>").append("\n");

        return sb.toString();
    }

    @Override
    public CloudMethodWorker getSchedulingCopy() {
        return new CloudMethodWorker(this);
    }

    @Override
    public boolean shouldBeStopped() {
        return getDescription().getTypeComposition().isEmpty();
    }

    @Override
    public <T extends WorkerResourceDescription> void destroyResources(T modification) {
        ResourceManager.terminateCloudResource(this, (CloudMethodResourceDescription) modification);
    }
}
