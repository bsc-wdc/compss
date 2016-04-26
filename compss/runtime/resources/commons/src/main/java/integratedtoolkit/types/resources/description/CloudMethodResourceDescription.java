package integratedtoolkit.types.resources.description;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.resources.MethodResourceDescription;
import java.util.LinkedList;
import org.w3c.dom.Node;

public class CloudMethodResourceDescription extends MethodResourceDescription {

    public static final CloudMethodResourceDescription EMPTY = new CloudMethodResourceDescription();
    //Resource Description
    private String name;
    private String type;
    private CloudImageDescription image;
    private String providerName;

    public CloudMethodResourceDescription() {
        super();
    }

    public CloudMethodResourceDescription(Constraints constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(MethodResourceDescription constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(CloudMethodResourceDescription clone) {
        super(clone);
        name = clone.name;
        type = clone.type;
        image = clone.image;
        providerName = clone.providerName;

    }

    public CloudMethodResourceDescription(Node n) {
        super();
        name = n.getAttributes().getNamedItem("Name").getTextContent();
        hostQueue = new LinkedList<String>();
        appSoftware = new LinkedList<String>();
        type = name;
        for (int i = 0; i < n.getChildNodes().getLength(); i++) {
            Node capabilities = n.getChildNodes().item(i);
            if (capabilities.getNodeName().compareTo("#text") == 0) {
            } else if (capabilities.getNodeName().compareTo("Capabilities") == 0) {
                for (int j = 0; j < capabilities.getChildNodes().getLength(); j++) {
                    if (capabilities.getChildNodes().item(j).getNodeName().compareTo("#text") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("Host") == 0) {
                        Node host = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < host.getChildNodes().getLength(); k++) {
                            if (host.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else if (host.getChildNodes().item(k).getNodeName().compareTo("TaskCount") == 0) {
                            } else if (host.getChildNodes().item(k).getNodeName().compareTo("Queue") == 0) {
                                hostQueue.add(host.getChildNodes().item(k).getTextContent());
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("Processor") == 0) {
                        Node processor = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < processor.getChildNodes().getLength(); k++) {
                            if (processor.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else if (processor.getChildNodes().item(k).getNodeName().compareTo("Architecture") == 0) {
                                processorArchitecture = processor.getChildNodes().item(k).getTextContent();
                            } else if (processor.getChildNodes().item(k).getNodeName().compareTo("CPUCount") == 0) {
                                processorCPUCount = Integer.parseInt(processor.getChildNodes().item(k).getTextContent());
                            } else if (processor.getChildNodes().item(k).getNodeName().compareTo("CoreCount") == 0) {
                                processorCoreCount = Integer.parseInt(processor.getChildNodes().item(k).getTextContent());
                                if (processorCPUCount == 0) {
                                    processorCPUCount = 1;
                                }
                            } else if (processor.getChildNodes().item(k).getNodeName().compareTo("Speed") == 0) {
                                processorSpeed = Float.parseFloat(processor.getChildNodes().item(k).getTextContent());
                            } else {
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("OS") == 0) {
                        Node OS = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < OS.getChildNodes().getLength(); k++) {
                            if (OS.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else if (OS.getChildNodes().item(k).getNodeName().compareTo("OSType") == 0) {
                                operatingSystemType = OS.getChildNodes().item(k).getTextContent();
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("StorageElement") == 0) {
                        Node storageElement = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < storageElement.getChildNodes().getLength(); k++) {
                            if (storageElement.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else if (storageElement.getChildNodes().item(k).getNodeName().compareTo("Size") == 0) {
                                storageElemSize = Float.parseFloat(storageElement.getChildNodes().item(k).getTextContent());
                            } else if (storageElement.getChildNodes().item(k).getNodeName().compareTo("AccessTime") == 0) {
                                storageElemAccessTime = Float.parseFloat(storageElement.getChildNodes().item(k).getTextContent());
                            } else if (storageElement.getChildNodes().item(k).getNodeName().compareTo("STR") == 0) {
                                storageElemSTR = Float.parseFloat(storageElement.getChildNodes().item(k).getTextContent());
                            } else {
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("Memory") == 0) {
                        Node memory = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < memory.getChildNodes().getLength(); k++) {
                            if (memory.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else if (memory.getChildNodes().item(k).getNodeName().compareTo("PhysicalSize") == 0) {
                                memoryPhysicalSize = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
                            } else if (memory.getChildNodes().item(k).getNodeName().compareTo("VirtualSize") == 0) {
                                memoryVirtualSize = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
                            } else if (memory.getChildNodes().item(k).getNodeName().compareTo("AccessTime") == 0) {
                                memoryAccessTime = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
                            } else if (memory.getChildNodes().item(k).getNodeName().compareTo("STR") == 0) {
                                memorySTR = Float.parseFloat(memory.getChildNodes().item(k).getTextContent());
                            } else {
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("ApplicationSoftware") == 0) {
                        Node apps = capabilities.getChildNodes().item(j);
                        for (int k = 0; k < apps.getChildNodes().getLength(); k++) {
                            if (apps.getChildNodes().item(k).getNodeName().compareTo("#text") == 0) {
                            } else {
                                Node soft = capabilities.getChildNodes().item(j);
                                for (int app = 0; app < soft.getChildNodes().getLength(); app++) {
                                    appSoftware.add(soft.getChildNodes().item(app).getTextContent());
                                }
                            }
                        }
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("Service") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("VO") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("Cluster") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("FileSystem") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("NetworkAdaptor") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("JobPolicy") == 0) {
                    } else if (capabilities.getChildNodes().item(j).getNodeName().compareTo("AccessControlPolicy") == 0) {
                    } else {
                    }
                }
            } else if (capabilities.getNodeName().compareTo("Price") == 0) {
                value = Float.parseFloat(capabilities.getTextContent());
            }
        }
    }

    @Override
    public CloudMethodResourceDescription copy() {
        return new CloudMethodResourceDescription(this);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //Cloud getters and setters
    public CloudImageDescription getImage() {
        return image;
    }

    public void setImage(CloudImageDescription image) {
        this.image = image;
    }

    public String getProviderName() {
        return this.providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public CloudMethodResourceDescription multiply(int amount) {
        CloudMethodResourceDescription rd = new CloudMethodResourceDescription();
        rd.processorCoreCount = this.processorCoreCount * amount;
        rd.processorCPUCount = this.processorCPUCount * amount;
        rd.memoryPhysicalSize = this.memoryPhysicalSize * amount;
        rd.memoryVirtualSize = this.memoryVirtualSize * amount;
        rd.storageElemSize = this.storageElemSize * amount;
        return rd;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("[CLOUD");
        sb.append(" PROVIDER =").append(this.providerName);
        sb.append(" IMAGE=").append((this.image == null) ? "NULL" : this.image.getName());
        sb.append(" TYPE=").append(this.type);
        sb.append("]");

        return sb.toString();
    }

}
