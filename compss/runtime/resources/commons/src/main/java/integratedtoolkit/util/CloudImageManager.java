package integratedtoolkit.util;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;


/**
 * The CloudImageManager is an utility to manage the different images that can
 * be used for a certain Cloud Provider
 */
public class CloudImageManager {

    /**
     * Relation between the name of an image and its features
     */
    private HashMap<String, CloudImageDescription> images;

    /**
     * Constructs a new CloudImageManager
     */
    public CloudImageManager() {
        images = new HashMap<String, CloudImageDescription>();
    }

    /**
     * Adds a new image which can be used by the Cloud Provider
     *
     * @param cid Description of the image
     */
    public void add(CloudImageDescription cid) {
        images.put(cid.getName(), cid);
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
        LinkedList<CloudImageDescription> compatiblesList = new LinkedList<CloudImageDescription>();
        for (CloudImageDescription cid : images.values()) {
            String imageArch = cid.getArch();
            String reqArch = requested.getProcessorArchitecture();
            if (imageArch.compareTo("[unassigned]") != 0
                    && reqArch.compareTo("[unassigned]") != 0
                    && imageArch.compareTo(reqArch) != 0) {
                continue;
            }

            String imageOS = cid.getOperativeSystem();
            String reqOS = requested.getOperatingSystemType();
            if (imageOS.compareTo("[unassigned]") != 0
                    && reqOS.compareTo("[unassigned]") != 0
                    && imageOS.compareTo(reqOS) != 0) {
                continue;
            }

            if (!cid.getSoftwareApps().containsAll(requested.getAppSoftware())) {
                continue;
            }

            compatiblesList.add(cid);
        }
        return compatiblesList;
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
        //IMAGES
        sb.append(prefix).append("IMAGES = [").append("\n");
        for (java.util.Map.Entry<String, CloudImageDescription> image : images.entrySet()) {
            String imageName = image.getKey();
            //CloudImageDescription description = image.getValue();
            sb.append(prefix).append("\t").append("IMAGE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("NAME = ").append(imageName).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
            sb.append(prefix).append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }
}
