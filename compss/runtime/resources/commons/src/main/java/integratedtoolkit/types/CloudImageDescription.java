package integratedtoolkit.types;

import integratedtoolkit.ITConstants;
import integratedtoolkit.util.ResourceManager;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;
import org.w3c.dom.Node;


public class CloudImageDescription {

    private final String name;
    private String providerName;

    private final HashMap<String, String> properties;

    private final LinkedList<String[]> packages;
    private final TreeSet<String> softwareApps;
    private String arch = "[unassigned]";
    private String operativeSystem = "[unassinged]";
    private String adaptor;
    
    private TreeMap<String, AdaptorDescription> adaptorsDescription = new TreeMap<>();

    private final HashMap<String, String> sharedDisks;

    
    public CloudImageDescription(String cloudProviderName, Node resourcesNode, Node projectNode, HashMap<String, String> providerProperties) {
        providerName = cloudProviderName;
        name = projectNode.getAttributes().getNamedItem("name").getTextContent();
        packages = new LinkedList<String[]>();
        sharedDisks = new HashMap<String, String>();
        softwareApps = new TreeSet<String>();
        properties = new HashMap<String, String>();
        for (int i = 0; i < projectNode.getChildNodes().getLength(); i++) {
            Node child = projectNode.getChildNodes().item(i);
            if (child.getNodeName().compareTo("InstallDir") == 0) {
                properties.put(ITConstants.INSTALL_DIR, child.getTextContent());
            } else if (child.getNodeName().compareTo("WorkingDir") == 0) {
                properties.put(ITConstants.WORKING_DIR, child.getTextContent());
            } else if (child.getNodeName().compareTo("AppDir") == 0) {
                properties.put(ITConstants.APP_DIR, child.getTextContent());
            } else if (child.getNodeName().compareTo("LibraryPath") == 0) {
                properties.put(ITConstants.LIB_PATH, child.getTextContent());
            } else if (child.getNodeName().compareTo("User") == 0) {
                properties.put(ITConstants.USER, child.getTextContent());
            } else if (child.getNodeName().compareTo("Password") == 0) {
                properties.put(ITConstants.PASSWORD, child.getTextContent());
            } else if (child.getNodeName().compareTo("CommAdaptor") == 0) {
                adaptor = child.getTextContent();
            } else if (child.getNodeName().compareTo("Package") == 0) {
                String[] p = new String[2];
                for (int j = 0; j < child.getChildNodes().getLength(); j++) {
                    Node packageChild = child.getChildNodes().item(j);
                    if (packageChild.getNodeName().compareTo("Source") == 0) {
                        p[0] = packageChild.getTextContent();
                    } else if (packageChild.getNodeName().compareTo("Target") == 0) {
                        p[1] = packageChild.getTextContent();
                    } else if (packageChild.getNodeName().compareTo("IncludedSoftware") == 0) {
                        for (int app = 0; app < packageChild.getChildNodes().getLength(); app++) {
                            Node appNode = packageChild.getChildNodes().item(app);
                            if (appNode.getNodeName().compareTo("Software") == 0) {
                                softwareApps.add(appNode.getTextContent());
                            }
                        }
                    }
                }
                packages.add(p);
            }
        }
        
        adaptorsDescription = ResourceManager.parseAdaptors(resourcesNode);

        for (int i = 0; i < resourcesNode.getChildNodes().getLength(); i++) {
            Node child = resourcesNode.getChildNodes().item(i);
            if (child.getNodeName().compareTo("ApplicationSoftware") == 0) {
                for (int app = 0; app < child.getChildNodes().getLength(); app++) {
                    Node appNode = child.getChildNodes().item(app);
                    if (appNode.getNodeName().compareTo("Software") == 0) {
                        softwareApps.add(appNode.getTextContent());
                    }
                }
            } else if (child.getNodeName().compareTo("SharedDisks") == 0) {
                for (int diskIndex = 0; diskIndex < child.getChildNodes().getLength(); diskIndex++) {
                    Node sharedDisk = child.getChildNodes().item(diskIndex);
                    if (sharedDisk.getNodeName().compareTo("Disk") == 0) {
                        String diskName = sharedDisk.getAttributes().getNamedItem("Name").getTextContent();
                        String mountPoint = "";
                        for (int j = 0; j < sharedDisk.getChildNodes().getLength(); j++) {
                            if (sharedDisk.getChildNodes().item(j).getNodeName().compareTo("MountPoint") == 0) {
                                mountPoint = sharedDisk.getChildNodes().item(j).getTextContent();
                            }
                        }
                        sharedDisks.put(diskName, mountPoint);
                    }
                }
            } else if (child.getNodeName().compareTo("OSType") == 0) {
                operativeSystem = child.getTextContent();
            } else if (child.getNodeName().compareTo("Architecture") == 0) {
                arch = child.getTextContent();
            } 
        }
        properties.putAll(providerProperties);
    }

    public String getName() {
        return this.name;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public String getArch() {
        return arch;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public LinkedList<String[]> getPackages() {
        return packages;
    }

    public HashMap<String, String> getSharedDisks() {
        return this.sharedDisks;
    }

    public String getOperativeSystem() {
        return this.operativeSystem;
    }

    public TreeSet<String> getSoftwareApps() {
        return softwareApps;
    }

    public String getAdaptor() {
        return adaptor;
    }
    
    public TreeMap<String, AdaptorDescription> getAdaptorsDescription(){
        return adaptorsDescription;
    }

    public String toString(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("IMAGE = [").append("\n");
        sb.append(prefix).append("\t").append("NAME = ").append(this.name).append("\n");
        sb.append(prefix).append("\t").append("COMM_ADAPTOR = ").append(this.adaptor).append("\n");
        sb.append(prefix).append("\t").append("ARCH = ").append(this.arch).append("\n");
        sb.append(prefix).append("\t").append("OS = ").append(this.operativeSystem).append("\n");
        sb.append(prefix).append("\t").append("INSTALL_DIR = ").append(properties.get(ITConstants.INSTALL_DIR)).append("\n");
        sb.append(prefix).append("\t").append("WORKING_DIR = ").append(properties.get(ITConstants.WORKING_DIR)).append("\n");
        sb.append(prefix).append("\t").append("APP_DIR = ").append(properties.get(ITConstants.APP_DIR)).append("\n");
        sb.append(prefix).append("\t").append("LIBRARY_PATH = ").append(properties.get(ITConstants.LIB_PATH)).append("\n");
        sb.append(prefix).append("\t").append("USER = ").append(properties.get(ITConstants.USER)).append("\n");
        sb.append(prefix).append("\t").append("PASSWORD = ").append(properties.get(ITConstants.PASSWORD)).append("\n");
        sb.append(prefix).append("\t").append("SHARED_DISKS = [").append("\n");
        for (java.util.Map.Entry<String, String> entry : this.sharedDisks.entrySet()) {
            sb.append(prefix).append("\t").append("\t").append("SHARED_DISK = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("DISK_NAME = ").append(entry.getKey()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("MOUNT_POINT = ").append(entry.getValue()).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("PACKAGES = ").append("\n");
        for (String[] pack : this.packages) {
            sb.append(prefix).append("\t").append("\t").append("PACKAGE = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("SOURCE = ").append(pack[0]).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("TARGET = ").append(pack[1]).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("SOFTWARE = ").append("\n");
        for (String app : this.softwareApps) {
            sb.append(prefix).append("\t").append("\t").append("APPLICATION = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("NAME = ").append(app).append("\n");
            sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        sb.append(prefix).append("]").append("\n");
        return sb.toString();
    }
}
