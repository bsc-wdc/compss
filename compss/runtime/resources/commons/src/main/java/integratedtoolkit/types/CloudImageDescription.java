package integratedtoolkit.types;

import integratedtoolkit.connectors.AbstractConnector;
import integratedtoolkit.connectors.AbstractSSHConnector;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;

import java.util.*;


public class CloudImageDescription {

    private final String providerName;
    private final String imageName;
    private final HashMap<String, String> properties;

    // Operating System
    private String operatingSystemType = MethodResourceDescription.UNASSIGNED_STR;
    private String operatingSystemDistribution = MethodResourceDescription.UNASSIGNED_STR;
    private String operatingSystemVersion = MethodResourceDescription.UNASSIGNED_STR;
    // Applications
    private List<String> appSoftware;
    // Packages
    private List<ApplicationPackage> packages;
    // SharedDisks
    private HashMap<String, String> sharedDisks;
    // Creation Time
    private int creationTime = MethodResourceDescription.UNASSIGNED_INT;
    // Price
    private int priceTimeUnit = MethodResourceDescription.UNASSIGNED_INT;
    private float pricePerUnit = MethodResourceDescription.UNASSIGNED_FLOAT;
    // Queues
    private List<String> queues;
    // Configuration
    private MethodConfiguration config;


    public CloudImageDescription(String cloudProviderName, String imageName, Map<String, String> providerProperties) {
        this.providerName = cloudProviderName;
        this.imageName = imageName;

        this.appSoftware = new LinkedList<>();
        this.packages = new LinkedList<>();
        this.sharedDisks = new HashMap<>();
        this.queues = new LinkedList<>();
        this.properties = new HashMap<>();

        this.properties.putAll(providerProperties);
    }

    public String getOperatingSystemType() {
        return operatingSystemType;
    }

    public void setOperatingSystemType(String operatingSystemType) {
        if (operatingSystemType != null && !operatingSystemType.isEmpty()) {
            this.operatingSystemType = operatingSystemType;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    public String getOperatingSystemDistribution() {
        return operatingSystemDistribution;
    }

    public void setOperatingSystemDistribution(String operatingSystemDistribution) {
        if (operatingSystemDistribution != null && !operatingSystemDistribution.isEmpty()) {
            this.operatingSystemDistribution = operatingSystemDistribution;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    public String getOperatingSystemVersion() {
        return operatingSystemVersion;
    }

    public void setOperatingSystemVersion(String operatingSystemVersion) {
        if (operatingSystemVersion != null && !operatingSystemVersion.isEmpty()) {
            this.operatingSystemVersion = operatingSystemVersion;
        } else {
            // Leave default UNASSIGNED VALUE
        }
    }

    public List<String> getAppSoftware() {
        return appSoftware;
    }

    public void setAppSoftware(List<String> appSoftware) {
        if (appSoftware != null) {
            this.appSoftware = appSoftware;
        }
    }

    public void addApplication(String app) {
        this.appSoftware.add(app);
    }

    public void addAllApplications(List<String> apps) {
        this.appSoftware.addAll(apps);
    }

    public List<String[]> getPackagesList() {
        LinkedList<String[]> packs = new LinkedList<>();
        for (ApplicationPackage p : this.packages) {
            String[] str_pack = new String[2];
            str_pack[0] = p.getSource();
            str_pack[1] = p.getTarget();
            packs.add(str_pack);
        }

        return packs;
    }

    public List<ApplicationPackage> getPackages() {
        return this.packages;
    }

    public void setPackages(List<ApplicationPackage> packages) {
        if (packages != null) {
            this.packages = new ArrayList<>(packages);
        }
    }

    public void addPackage(String source, String target) {
        ApplicationPackage p = new ApplicationPackage(source, target);
        this.packages.add(p);
    }

    public HashMap<String, String> getSharedDisks() {
        return sharedDisks;
    }

    public void setSharedDisks(HashMap<String, String> sharedDisks) {
        if (sharedDisks != null && !sharedDisks.isEmpty()) {
            this.sharedDisks = new HashMap<>(sharedDisks);
        }
    }

    public int getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(int creationTime) {
        this.creationTime = creationTime;
    }

    public int getPriceTimeUnit() {
        return priceTimeUnit;
    }

    public void setPriceTimeUnit(int priceTimeUnit) {
        this.priceTimeUnit = priceTimeUnit;
    }

    public float getPricePerUnit() {
        return pricePerUnit;
    }

    public void setPricePerUnit(float pricePerUnit) {
        this.pricePerUnit = pricePerUnit;
    }

    public List<String> getQueues() {
        return queues;
    }

    public void setQueues(List<String> queues) {
        if (queues != null) {
            this.queues = new ArrayList<>(queues);
        }
    }

    public void addQueue(String queue) {
        this.queues.add(queue);
    }

    public MethodConfiguration getConfig() {
        return config;
    }

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

    public String getProviderName() {
        return providerName;
    }

    public String getImageName() {
        return imageName;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

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
        sb.append(prefix).append("\t").append("USER = ").append(this.getConfig().getUser()).append("\n");
        sb.append(prefix).append("\t").append("PASSWORD = ").append(this.getProperties()
                .get(AbstractSSHConnector.PROPERTY_PASSW_NAME)).append("\n");
        sb.append(prefix).append("\t").append("SHARED_DISKS = [").append("\n");
        for (java.util.Map.Entry<String, String> entry : this.sharedDisks.entrySet()) {
            sb.append(prefix).append("\t").append("\t").append("SHARED_DISK = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("DISK_NAME = ").append(entry.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("MOUNT_POINT = ").append(entry.getValue()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("PACKAGES = [").append("\n");
        for (ApplicationPackage pack : this.packages) {
            sb.append(prefix).append("\t").append("\t").append("PACKAGE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("SOURCE = ").append(pack.getSource()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("TARGET = ").append(pack.getTarget()).append("\n");
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
