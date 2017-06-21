package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The CloudImageManager is an utility to manage the different images that can
 * be used for a certain Cloud Provider
 */
public class CloudImageManager {

    /**
     * Relation between the name of an image and its features
     */
    private HashMap<String, CloudImageDescription> images;

    // Logger
    private static final Logger logger = LogManager.getLogger(Loggers.CM_COMP);

    /**
     * Constructs a new CloudImageManager
     */
    public CloudImageManager() {
        logger.info("Initializing CloudImageManager");
        images = new HashMap<>();
    }

    /**
     * Adds a new image which can be used by the Cloud Provider
     *
     * @param cid Description of the image
     */
    public void add(CloudImageDescription cid) {
        logger.debug("Add new Image description");
        images.put(cid.getImageName(), cid);
    }

    /**
     * Finds all the images provided by the Cloud Provider which fulfill the
     * resource description.
     *
     * @param requested description of the features that the image must provide
     * @return The best image provided by the Cloud Provider which fulfills the
     * resource description
     */
    public LinkedList<CloudImageDescription> getCompatibleImages(MethodResourceDescription requested) {
        // logger.debug("REQUESTED: " + requested.toString());
        LinkedList<CloudImageDescription> compatiblesList = new LinkedList<>();
        for (CloudImageDescription cid : images.values()) {
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
                    && !reqOSDistr.equals(CloudMethodResourceDescription.UNASSIGNED_STR) && !imageOSDistr.equals(reqOSDistr)) {
                continue;
            }

            String imageOSVersion = cid.getOperatingSystemVersion();
            String reqOSVersion = requested.getOperatingSystemVersion();
            if (!imageOSVersion.equals(CloudMethodResourceDescription.UNASSIGNED_STR)
                    && !reqOSVersion.equals(CloudMethodResourceDescription.UNASSIGNED_STR) && !imageOSVersion.equals(reqOSVersion)) {
                continue;
            }

            // SOFTWARE CHECK
            if (!cid.getAppSoftware().containsAll(requested.getAppSoftware())) {
                continue;
            }

            // CHECK QUEUES
            List<String> req_queues = requested.getHostQueues();
            List<String> image_queues = cid.getQueues();
            // Disjoint = true if the two specified collections have no elements in common.
            if (!req_queues.isEmpty() && Collections.disjoint(req_queues, image_queues)) {
                continue;
            }

            compatiblesList.add(cid);
        }
        return compatiblesList;
    }

    /**
     *
     * @param name
     * @return Returns the Image associated to that name
     */
    public CloudImageDescription getImage(String name) {
        return images.get(name);
    }

    /**
     * Return all the image names offered by that Cloud Provider
     *
     * @return set of image names offered by that Cloud Provider
     */
    public Set<String> getAllImageNames() {
        return images.keySet();
    }

    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // IMAGES
        sb.append(prefix).append("IMAGES = [").append("\n");
        for (java.util.Map.Entry<String, CloudImageDescription> image : images.entrySet()) {
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
