package integratedtoolkit.types.resources.description;

import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.annotations.Constraints;
import integratedtoolkit.types.resources.MethodResourceDescription;
import java.util.LinkedList;
import java.util.List;
import org.w3c.dom.Node;


public class CloudMethodResourceDescription extends MethodResourceDescription {

    public static final CloudMethodResourceDescription EMPTY = new CloudMethodResourceDescription();
    //Resource Description
    private String name;
    private String type;
    private CloudImageDescription image;
    private String providerName;

    //Extra fields
    int slots = 0;
    private Float value = 0.0f;

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

    public int getSlots() {
        return slots;
    }

    public void setSlots(int qty) {
        slots = qty;
    }

    public void addSlot() {
        slots++;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    //Processor getters and setters
    public String getProcessorArchitecture() {
        return processorArchitecture;
    }

    public void setProcessorArchitecture(String Architecture) {
        processorArchitecture = Architecture;
    }

    public int getProcessorCPUCount() {
        return processorCPUCount;
    }

    public void setProcessorCPUCount(int count) {
        processorCPUCount = count;
    }

    public int getProcessorCoreCount() {
        return processorCoreCount;
    }

    public void setProcessorCoreCount(int count) {
        processorCoreCount = count;
    }

    public float getProcessorSpeed() {
        return processorSpeed;
    }

    public void setProcessorSpeed(float GHz) {
        processorSpeed = GHz;
    }

    //Memory getters and setters   
    public float getMemoryPhysicalSize() {
        return memoryPhysicalSize;
    }

    public void setMemoryPhysicalSize(float GB) {
        memoryPhysicalSize = GB;
    }

    public float getMemoryVirtualSize() {
        return memoryVirtualSize;
    }

    public void setMemoryVirtualSize(float GB) {
        memoryVirtualSize = GB;
    }

    public float getMemoryAccessTime() {
        return memoryAccessTime;
    }

    public void setMemoryAccessTime(float ns) {
        memoryAccessTime = ns;
    }

    public float getMemorySTR() {
        return memorySTR;
    }

    public void setMemorySTR(float GBs) {
        memorySTR = GBs;
    }

    //Storage getters and setters
    public float getStorageElemSize() {
        return storageElemSize;
    }

    public void setStorageElemSize(float GB) {
        storageElemSize = GB;
    }

    public float getStorageElemAccessTime() {
        return storageElemAccessTime;
    }

    public void setStorageElemAccessTime(float ms) {
        storageElemAccessTime = ms;
    }

    public float getStorageElemSTR() {
        return storageElemSTR;
    }

    public void setStorageElemSTR(float MBs) {
        storageElemSTR = MBs;
    }

    //Host queue getters and setters
    public List<String> getHostQueue() {
        return hostQueue;
    }

    public void setHostQueue(List<String> hostQueue) {
        this.hostQueue = new LinkedList<String>(hostQueue);
    }

    public void addHostQueue(String queue) {
        this.hostQueue.add(queue);
    }

    //Operating System Type getters and setters
    public String getOperatingSystemType() {
        return operatingSystemType;
    }

    public void setOperatingSystemType(String OSType) {
        operatingSystemType = OSType;
    }

    //App Software getters and setters
    public List<String> getAppSoftware() {
        return appSoftware;
    }

    public void setAppSoftware(List<String> appSoftware) {
        this.appSoftware = new LinkedList<String>(appSoftware);
    }

    public void resetAppSoftware() {
        appSoftware = new LinkedList<String>();
    }

    public void addAppSoftware(String software) {
        appSoftware.add(software);
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

    public Float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }

    public void join(CloudMethodResourceDescription mr2) {
        this.processorCPUCount = java.lang.Math.max(this.processorCPUCount, mr2.processorCPUCount);
        this.processorCoreCount = java.lang.Math.max(this.processorCoreCount, mr2.processorCoreCount);
        this.processorSpeed = java.lang.Math.max(this.processorSpeed, mr2.processorSpeed);

        this.memoryPhysicalSize = java.lang.Math.max(this.memoryPhysicalSize, mr2.memoryPhysicalSize);
        this.memoryVirtualSize = java.lang.Math.max(this.memoryVirtualSize, mr2.memoryVirtualSize);
        this.memoryAccessTime = java.lang.Math.max(this.memoryAccessTime, mr2.memoryAccessTime);
        this.memorySTR = java.lang.Math.max(this.memorySTR, mr2.memorySTR);

        this.storageElemSize = java.lang.Math.max(this.storageElemSize, mr2.storageElemSize);
        this.storageElemAccessTime = java.lang.Math.max(this.storageElemAccessTime, mr2.storageElemAccessTime);
        this.storageElemSTR = java.lang.Math.max(this.storageElemSTR, mr2.storageElemSTR);

        if (this.processorArchitecture.compareTo("[unassigned]") == 0) {
            this.processorArchitecture = mr2.processorArchitecture;
        }
        if (this.operatingSystemType.compareTo("[unassigned]") == 0) {
            this.operatingSystemType = mr2.operatingSystemType;
        }

        for (int i = 0; i < mr2.hostQueue.size(); i++) {
            if (!this.hostQueue.contains(mr2.hostQueue.get(i))) {
                this.hostQueue.add(mr2.hostQueue.get(i));
            }
        }

        for (int i = 0; i < mr2.appSoftware.size(); i++) {
            if (!this.appSoftware.contains(mr2.appSoftware.get(i))) {
                this.appSoftware.add(mr2.appSoftware.get(i));
            }
        }

        this.slots += mr2.slots;
    }

    public float difference(CloudMethodResourceDescription mr2) {
        float processor_dif = this.processorCoreCount - mr2.processorCoreCount;
        float memory_dif = this.memoryPhysicalSize - mr2.memoryPhysicalSize;
        return processor_dif * 10000 + memory_dif;
    }

    public boolean contains(CloudMethodResourceDescription rc2) {
        return (this.operatingSystemType.compareTo(rc2.operatingSystemType) == 0 || this.operatingSystemType.compareTo("[unassigned]") == 0 || ("[unassigned]").compareTo(rc2.operatingSystemType) == 0)
                && (this.processorArchitecture.compareTo(rc2.processorArchitecture) == 0 || this.processorArchitecture.compareTo("[unassigned]") == 0 || ("[unassigned]").compareTo(rc2.processorArchitecture) == 0)
                && (this.hostQueue.containsAll(rc2.hostQueue))
                && (this.appSoftware.containsAll(rc2.appSoftware))
                && !(this.processorCoreCount < rc2.processorCoreCount
                || this.processorCPUCount < rc2.processorCPUCount
                || this.processorSpeed < rc2.processorSpeed
                || this.memoryPhysicalSize < rc2.memoryPhysicalSize
                || this.memoryVirtualSize < rc2.memoryVirtualSize
                || this.memorySTR < rc2.memorySTR
                || this.memoryAccessTime < rc2.memoryAccessTime
                || this.storageElemAccessTime < rc2.storageElemAccessTime
                || this.storageElemSTR < rc2.storageElemSTR
                || this.storageElemSize < rc2.storageElemSize);
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

    public void increase(CloudMethodResourceDescription rd) {
        this.processorCPUCount += rd.processorCPUCount;
        this.processorCoreCount += rd.processorCoreCount;
        this.memoryPhysicalSize += rd.memoryPhysicalSize;
        this.memoryVirtualSize += rd.memoryVirtualSize;
        this.storageElemSize += rd.storageElemSize;
    }

    public void reduce(CloudMethodResourceDescription rd) {
        this.processorCPUCount -= rd.processorCPUCount;
        this.processorCoreCount -= rd.processorCoreCount;
        this.memoryPhysicalSize -= rd.memoryPhysicalSize;
        this.memoryVirtualSize -= rd.memoryVirtualSize;
        this.storageElemSize -= rd.storageElemSize;
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
