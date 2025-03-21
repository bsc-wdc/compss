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
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The CloudImageManager is an utility to manage the different images that can be used for a certain Cloud Provider.
 */
public class CloudImageManager {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CM_COMP);

    // Relation between the name of an image and its features
    private Map<String, CloudImageDescription> images;


    /**
     * Constructs a new CloudImageManager.
     */
    public CloudImageManager() {
        LOGGER.debug("Initializing CloudImageManager");
        this.images = new HashMap<>();
    }

    /**
     * Adds a new image which can be used by the Cloud Provider.
     *
     * @param cid Description of the image.
     */
    public void add(CloudImageDescription cid) {
        LOGGER.debug("Add new Image description " + cid.getImageName());
        this.images.put(cid.getImageName(), cid);
    }

    /**
     * Finds all the images provided by the Cloud Provider which fulfill the resource description.
     *
     * @param requested Description of the features that the image must provide.
     * @return The best image provided by the Cloud Provider which fulfills the resource description.
     */
    public List<CloudImageDescription> getCompatibleImages(MethodResourceDescription requested) {
        // logger.debug("REQUESTED: " + requested.toString());
        List<CloudImageDescription> compatiblesList = new LinkedList<>();
        for (CloudImageDescription cid : this.images.values()) {
            // logger.debug("CID: " + cid.toString());

            // OS CHECK
            String imageOSType = cid.getOperatingSystemType();
            String reqOSType = requested.getOperatingSystemType();
            if (!imageOSType.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                && !reqOSType.equals(CloudMethodResourceDescription.UNASSIGNED_STR) && !imageOSType.equals(reqOSType)) {
                continue;
            }

            String imageOSDistr = cid.getOperatingSystemDistribution();
            String reqOSDistr = requested.getOperatingSystemDistribution();
            if (!imageOSDistr.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                && !reqOSDistr.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                && !imageOSDistr.equals(reqOSDistr)) {
                continue;
            }

            String imageOSVersion = cid.getOperatingSystemVersion();
            String reqOSVersion = requested.getOperatingSystemVersion();
            if (!imageOSVersion.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                && !reqOSVersion.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                && !imageOSVersion.equals(reqOSVersion)) {
                continue;
            }

            // SOFTWARE CHECK
            if (!cid.getAppSoftware().containsAll(requested.getAppSoftware())) {
                continue;
            }

            // CHECK QUEUES
            List<String> reqQueues = requested.getHostQueues();
            List<String> imageQueues = cid.getQueues();
            // Disjoint = true if the two specified collections have no elements in common.
            if (!reqQueues.isEmpty() && Collections.disjoint(reqQueues, imageQueues)) {
                continue;
            }

            compatiblesList.add(cid);
        }
        return compatiblesList;
    }

    /**
     * Returns the image associated with the given name.
     * 
     * @param name Image name.
     * @return Returns the Image associated to that name.
     */
    public CloudImageDescription getImage(String name) {
        return this.images.get(name);
    }

    /**
     * Returns all the image names offered by that Cloud Provider.
     *
     * @return Set of image names offered by that Cloud Provider.
     */
    public Set<String> getAllImageNames() {
        return this.images.keySet();
    }

    /**
     * Returns all the images offered by that Cloud Provider.
     *
     * @return Set of images offered by that Cloud Provider.
     */
    public Collection<CloudImageDescription> getAllImages() {
        return this.images.values();
    }

    /**
     * Dumps the current state.
     * 
     * @param prefix Prefix to append.
     * @return String containing the dump of the current state.
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // IMAGES
        sb.append(prefix).append("IMAGES = [").append("\n");
        for (java.util.Map.Entry<String, CloudImageDescription> image : this.images.entrySet()) {
            String imageName = image.getKey();
            // CloudImageDescription description = image.getValue();
            sb.append(prefix).append("\t").append("IMAGE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("NAME = ").append(imageName).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

}
