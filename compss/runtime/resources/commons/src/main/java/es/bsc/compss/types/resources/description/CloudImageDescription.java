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
package es.bsc.compss.types.resources.description;

import es.bsc.compss.connectors.AbstractConnector;
import es.bsc.compss.connectors.AbstractSSHConnector;
import es.bsc.compss.types.ApplicationPackage;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class CloudImageDescription {

    private final String imageName;
    private final Map<String, String> properties;

    // Operating System
    private String operatingSystemType = MethodResourceDescription.UNASSIGNED_STR;
    private String operatingSystemDistribution = MethodResourceDescription.UNASSIGNED_STR;
    private String operatingSystemVersion = MethodResourceDescription.UNASSIGNED_STR;
    // Applications
    private List<String> appSoftware;
    // Packages
    private List<ApplicationPackage> packages;
    // SharedDisks
    private Map<String, String> sharedDisks;
    // Creation Time
    private int creationTime = MethodResourceDescription.UNASSIGNED_INT;
    // Price
    private int priceTimeUnit = MethodResourceDescription.UNASSIGNED_INT;
    private float pricePerUnit = MethodResourceDescription.UNASSIGNED_FLOAT;
    // Queues
    private List<String> queues;
    // Configuration
    private MethodConfiguration config;


    /**
     * Creates a new CloudImageDescription with the given name and properties.
     * 
     * @param imageName Image name.
     * @param providerProperties Cloud provider specific properties.
     */
    public CloudImageDescription(String imageName, Map<String, String> providerProperties) {
        this.imageName = imageName;

        this.appSoftware = new LinkedList<>();
        this.packages = new LinkedList<>();
        this.sharedDisks = new HashMap<>();
        this.queues = new LinkedList<>();
        this.properties = new HashMap<>();

        this.properties.putAll(providerProperties);
    }

    /**
     * Returns the operating system of the image.
     * 
     * @return The operating system of the image.
     */
    public String getOperatingSystemType() {
        return this.operatingSystemType;
    }

    /**
     * Sets a new operating system of the image.
     * 
     * @param operatingSystemType New operating system.
     */
    public void setOperatingSystemType(String operatingSystemType) {
        if (operatingSystemType != null && !operatingSystemType.isEmpty()) {
            this.operatingSystemType = operatingSystemType;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    /**
     * Returns the operating system distribution of the image.
     * 
     * @return The operating system distribution of the image.
     */
    public String getOperatingSystemDistribution() {
        return this.operatingSystemDistribution;
    }

    /**
     * Sets a new operating system distribution.
     * 
     * @param operatingSystemDistribution New operating system distribution.
     */
    public void setOperatingSystemDistribution(String operatingSystemDistribution) {
        if (operatingSystemDistribution != null && !operatingSystemDistribution.isEmpty()) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    /**
     * Returns the operating system version of the image.
     * 
     * @return The operating system version of the image.
     */
    public String getOperatingSystemVersion() {
        return this.operatingSystemVersion;
    }

    /**
     * Sets a new operating system version of the image.
     * 
     * @param operatingSystemVersion The new operating system version of the image.
     */
    public void setOperatingSystemVersion(String operatingSystemVersion) {
        if (operatingSystemVersion != null && !operatingSystemVersion.isEmpty()) {
            this.operatingSystemVersion = operatingSystemVersion;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    /**
     * Returns the application software available at the image.
     * 
     * @return The application software available at the image.
     */
    public List<String> getAppSoftware() {
        return this.appSoftware;
    }

    /**
     * Sets a new list of application software available at the image.
     * 
     * @param appSoftware New list of application software available at the image.
     */
    public void setAppSoftware(List<String> appSoftware) {
        if (appSoftware != null) {
            this.appSoftware = appSoftware;
        }
    }

    /**
     * Adds a new application software available at the image.
     * 
     * @param app New application software.
     */
    public void addApplication(String app) {
        this.appSoftware.add(app);
    }

    /**
     * Adds all the given application software available at the image.
     * 
     * @param apps List of new application software.
     */
    public void addAllApplications(List<String> apps) {
        this.appSoftware.addAll(apps);
    }

    /**
     * Returns a list of source and target of each package available at the image.
     * 
     * @return A list of source and target of each package available at the image.
     */
    public List<String[]> getPackagesList() {
        List<String[]> packs = new LinkedList<>();
        for (ApplicationPackage p : this.packages) {
            String[] strPack = new String[2];
            strPack[0] = p.getSource();
            strPack[1] = p.getTarget();
            packs.add(strPack);
        }

        return packs;
    }

    /**
     * Returns a list of packages available at the image.
     * 
     * @return A list of packages available at the image.
     */
    public List<ApplicationPackage> getPackages() {
        return this.packages;
    }

    /**
     * Sets a new list of packages available at the image.
     * 
     * @param packages New list of packages available at the image.
     */
    public void setPackages(List<ApplicationPackage> packages) {
        if (packages != null) {
            this.packages = new ArrayList<>(packages);
        }
    }

    /**
     * Adds a new package to the image with the given source and target.
     * 
     * @param source Package source destination.
     * @param target Package target destination.
     */
    public void addPackage(String source, String target) {
        ApplicationPackage p = new ApplicationPackage(source, target);
        this.packages.add(p);
    }

    /**
     * Returns the shared disks mounted on the image.
     * 
     * @return The shared disks mounted on the image.
     */
    public Map<String, String> getSharedDisks() {
        return this.sharedDisks;
    }

    /**
     * Sets a new map of shared disks mounted on the image.
     * 
     * @param sharedDisks New map of shared disks mounted on the image.
     */
    public void setSharedDisks(Map<String, String> sharedDisks) {
        if (sharedDisks != null && !sharedDisks.isEmpty()) {
            this.sharedDisks = new HashMap<>(sharedDisks);
        }
    }

    /**
     * Returns the image creation time.
     * 
     * @return The image creation time.
     */
    public int getCreationTime() {
        return this.creationTime;
    }

    /**
     * Sets a new image creation time.
     * 
     * @param creationTime New image creation time.
     */
    public void setCreationTime(int creationTime) {
        this.creationTime = creationTime;
    }

    /**
     * Returns the price time unit.
     * 
     * @return The price time unit.
     */
    public int getPriceTimeUnit() {
        return this.priceTimeUnit;
    }

    /**
     * Sets a new price time unit.
     * 
     * @param priceTimeUnit New price time unit.
     */
    public void setPriceTimeUnit(int priceTimeUnit) {
        this.priceTimeUnit = priceTimeUnit;
    }

    /**
     * Returns the price per time unit.
     * 
     * @return The price per time unit.
     */
    public float getPricePerUnit() {
        return this.pricePerUnit;
    }

    /**
     * Sets a new price per time unit.
     * 
     * @param pricePerUnit New price per time unit.
     */
    public void setPricePerUnit(float pricePerUnit) {
        this.pricePerUnit = pricePerUnit;
    }

    /**
     * Returns the available queues at the image.
     * 
     * @return The available queues at the image.
     */
    public List<String> getQueues() {
        return this.queues;
    }

    /**
     * Sets a new list of available queues at the image.
     * 
     * @param queues New list of available queues at the image.
     */
    public void setQueues(List<String> queues) {
        if (queues != null) {
            this.queues = new LinkedList<>(queues);
        }
    }

    /**
     * Adds a new queue to the available queues at the image.
     * 
     * @param queue Queue to add.
     */
    public void addQueue(String queue) {
        this.queues.add(queue);
    }

    /**
     * Returns the image method configuration.
     * 
     * @return The image method configuration.
     */
    public MethodConfiguration getConfig() {
        return this.config;
    }

    /**
     * Sets a new image method configuration.
     * 
     * @param config Method configuration.
     */
    public void setConfig(MethodConfiguration config) {
        this.config = config.copy();

        // Add adaptor ports in image properties for some connectors (i.e. JClouds)
        String maxPort = "-1";
        String minPort = "-1";
        if (this.config != null) {
            maxPort = String.valueOf(this.config.getMaxPort());
            minPort = String.valueOf(this.config.getMinPort());
        }
        this.properties.put(AbstractConnector.ADAPTOR_MAX_PORT_PROPERTY_NAME, maxPort);
        this.properties.put(AbstractConnector.ADAPTOR_MIN_PORT_PROPERTY_NAME, minPort);
    }

    /**
     * Returns the image name.
     * 
     * @return The image name.
     */
    public String getImageName() {
        return this.imageName;
    }

    /**
     * Returns the image specific properties.
     * 
     * @return The image specific properties.
     */
    public Map<String, String> getProperties() {
        return this.properties;
    }

    /**
     * Dumps the current image information with the given prefix.
     * 
     * @param prefix Indentation prefix.
     * @return String containing a dump of the current image information.
     */
    public String toString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("IMAGE = [").append("\n");
        sb.append(prefix).append("\t").append("NAME = ").append(this.imageName).append("\n");
        sb.append(prefix).append("\t").append("OS_TYPE = ").append(this.getOperatingSystemType()).append("\n");
        sb.append(prefix).append("\t").append("OS_DISTR = ").append(this.getOperatingSystemDistribution()).append("\n");
        sb.append(prefix).append("\t").append("OS_VERSION = ").append(this.getOperatingSystemVersion()).append("\n");
        sb.append(prefix).append("\t").append("INSTALL_DIR = ").append(this.getConfig().getInstallDir()).append("\n");
        sb.append(prefix).append("\t").append("WORKING_DIR = ").append(this.getConfig().getWorkingDir()).append("\n");
        sb.append(prefix).append("\t").append("APP_DIR = ").append(this.getConfig().getAppDir()).append("\n");
        sb.append(prefix).append("\t").append("LIBRARY_PATH = ").append(this.getConfig().getLibraryPath()).append("\n");
        sb.append(prefix).append("\t").append("CLASSPATH = ").append(this.getConfig().getClasspath()).append("\n");
        sb.append(prefix).append("\t").append("PYTHONPATH = ").append(this.getConfig().getPythonpath()).append("\n");
        sb.append(prefix).append("\t").append("ENV_SCRIPT = ").append(this.getConfig().getEnvScript()).append("\n");
        sb.append(prefix).append("\t").append("USER = ").append(this.getConfig().getUser()).append("\n");
        sb.append(prefix).append("\t").append("PASSWORD = ")
            .append(this.getProperties().get(AbstractSSHConnector.PROPERTY_PASSW_NAME)).append("\n");
        sb.append(prefix).append("\t").append("SHARED_DISKS = [").append("\n");
        for (Entry<String, String> entry : this.sharedDisks.entrySet()) {
            sb.append(prefix).append("\t").append("\t").append("SHARED_DISK = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("DISK_NAME = ").append(entry.getKey())
                .append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("MOUNT_POINT = ").append(entry.getValue())
                .append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("PACKAGES = [").append("\n");
        for (ApplicationPackage pack : this.packages) {
            sb.append(prefix).append("\t").append("\t").append("PACKAGE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("SOURCE = ").append(pack.getSource())
                .append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("TARGET = ").append(pack.getTarget())
                .append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("SOFTWARE = [").append("\n");
        for (String app : this.appSoftware) {
            sb.append(prefix).append("\t").append("\t").append("APPLICATION = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("NAME = ").append(app).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        sb.append(prefix).append("]").append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toString("");
    }

}
