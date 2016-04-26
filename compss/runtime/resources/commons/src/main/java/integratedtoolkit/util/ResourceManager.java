package integratedtoolkit.util;

import java.util.LinkedList;

import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.components.ResourceUser.WorkloadStatus;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.ResourcesState;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.ServiceResourceDescription;
import integratedtoolkit.types.resources.ServiceWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.Worker;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import org.xml.sax.SAXException;

/**
 * The ResourceManager class is an utility to manage all the resources available
 * for the cores execution. It keeps information about the features of each
 * resource and is used as an endpoint to discover which resources can run a
 * core in a certain moment, the total and the available number of slots.
 *
 */
public class ResourceManager {

    protected static final String ERROR_UNKNOWN_HOST = "ERROR: Cannot determine the IP address of the local host";
    private static final String PROJ_LOAD_ERR = "Error loading project information";
    private static final String DEL_VM_ERR = "Error deleting VMs";

    //XML Document
    private static Document resourcesDoc;

    //Information about resources
    private static WorkerPool pool;
    private static int[] poolCoreMaxConcurrentTasks;
    private static ResourceUser resourceUser;
    private static ResourceOptimizer ro;

    private static final Logger resourcesLogger = Logger.getLogger(Loggers.RESOURCES);
    private static final Logger runtimeLogger = Logger.getLogger(Loggers.RM_COMP);

    /**
     * Constructs a new ResourceManager using the Resources xml file content.
     * First of all, an empty resource pool is created and the Cloud Manager is
     * initialized without any providers. Secondly the resource file is
     * validated and parsed and the toplevel xml nodes are processed in
     * different ways depending on its type: - Resource: a new Physical resource
     * is added to the resource pool with the same id as its Name attribute and
     * as many slots as indicated in the project file. If it has 0 slots or it
     * is not on the project xml, the resource is not included.
     *
     * - Service: a new Physical resource is added to the resource pool with the
     * same id as its wsdl attribute and as many slots as indicated in the
     * project file. If it has 0 slots or it is not on the project xml, the
     * resource is not included.
     *
     * - Cloud Provider: if there is any CloudProvider in the project file with
     * the same name, a new Cloud Provider is added to the CloudManager with its
     * name attribute value as identifier. The CloudManager is configured as
     * described between the project xml and the resources file. From the
     * resource file it gets the properties which describe how to connect with
     * it: the connector path, the endpoint, ... Other properties required to
     * manage the resources are specified on the project file: i.e. the maximum
     * amount of resource deployed on that provider. Some configurations depend
     * on both files. One of them is the list of usable images. The images
     * offered by the cloud provider are on a list on the resources file, where
     * there are specified the name and the software description of that image.
     * On the project file there is a description of how the resources created
     * with that image must be used: username, working directory,... Only the
     * images that have been described in both files are added to the
     * CloudManager
     *
     *
     * @param resUser class to notify resource changes
     *
     * @throws Exception Parsing the xml file or creating new instances for the
     * Cloud providers connectors
     */
    public static void load(ResourceUser resUser) throws Exception {
        pool = new WorkerPool();
        poolCoreMaxConcurrentTasks = new int[CoreManager.getCoreCount()];
        if (!ProjectManager.isInit()) {
            try {
                ProjectManager.init();
            } catch (Exception e) {
                resourcesLogger.fatal(PROJ_LOAD_ERR, e);
                runtimeLogger.fatal(PROJ_LOAD_ERR, e);
                System.exit(1);
            }
        }

        CloudManager.initialize();
        resourceUser = resUser;

        String resourceFile = System.getProperty(ITConstants.IT_RES_FILE);
        // Parse the XML document which contains resource information
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);

        resourcesDoc = docFactory.newDocumentBuilder().parse(resourceFile);

        // Validate the document against an XML Schema
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Source schemaFile = new StreamSource(System.getProperty(ITConstants.IT_RES_SCHEMA));
        Schema schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        // validate the DOM tree
        try {
            validator.validate(new DOMSource(resourcesDoc));
        } catch (SAXException e) {
            runtimeLogger.error("Error Validating resources.xml config file.\n", e);
            System.err.println("Error Validating resources.xml config file.\n" + e.getMessage());
        } catch (IOException e) {
            runtimeLogger.error("IOError while trying to validate resources.xml config file.\n", e);
            System.err.println("IOError while trying to validate resources.xml config file.\n" + e.getMessage());
        }
        validator.validate(new DOMSource(resourcesDoc));

        // resolver = evaluator.createNSResolver(resourcesDoc);
        NodeList nl = resourcesDoc.getChildNodes().item(0).getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeName().compareTo("Resource") == 0) {
                String name = n.getAttributes().getNamedItem("Name").getTextContent();
                if (!ProjectManager.containsWorker(name)) {
                    continue;
                }
                loadMethodWorker(name, n);
            } else if (n.getNodeName().compareTo("Service") == 0) {
                String name = n.getAttributes().getNamedItem("wsdl").getTextContent();
                if (!ProjectManager.containsWorker(name)) {
                    continue;
                }
                try {
                    loadService(name, n);
                } catch (Exception e) {
                    resourcesLogger.error("Could not load service " + name);
                }
            } else if (n.getNodeName().compareTo("CloudProvider") == 0) {
                String cloudProviderName = n.getAttributes().getNamedItem("name").getTextContent();
                if (!ProjectManager.existsCloudProvider(cloudProviderName)) {
                    runtimeLogger.debug("Provider " + cloudProviderName + " not in project.xml");
                    continue;
                }
                loadCloudProvider(cloudProviderName, n);
            } else if (n.getNodeName().compareTo("Disk") == 0) {
                String diskName = n.getAttributes().getNamedItem("Name").getTextContent();
                String mountPoint = "";
                for (int j = 0; j < n.getChildNodes().getLength(); j++) {
                    if (n.getChildNodes().item(j).getNodeName().compareTo("Name") == 0) {
                        diskName = n.getChildNodes().item(j).getTextContent();
                    } else if (n.getChildNodes().item(j).getNodeName().compareTo("MountPoint") == 0) {
                        mountPoint = n.getChildNodes().item(j).getTextContent();
                    }
                }
                Comm.addSharedDiskToMaster(diskName, mountPoint);
            } else if (n.getNodeName().compareTo("DataNode") == 0) {
                String host = "";
                String path = "";
                for (int j = 0; j < n.getChildNodes().getLength(); j++) {
                    if (n.getChildNodes().item(j).getNodeName().compareTo("Host") == 0) {
                        host = n.getChildNodes().item(j).getTextContent();
                    } else if (n.getChildNodes().item(j).getNodeName().compareTo("Path") == 0) {
                        path = n.getChildNodes().item(j).getTextContent();
                    }
                }
                runtimeLogger.debug("DataNode: Host = " + host + " , Path = " + path);
            }
        }
        ro = new ResourceOptimizer(resourceUser);
        ro.setName("Resource Optimizer");
        ro.start();
    }

    private static void loadMethodWorker(String name, Node n) throws Exception {
        MethodResourceDescription rd = new MethodResourceDescription(n);
        TreeMap<String, AdaptorDescription> resourceAdaptorsDesc = parseAdaptors(n);
        TreeMap<String, AdaptorDescription> projectAdaptorsDesc = ProjectManager.parseAdaptors();
        TreeMap<String, AdaptorDescription> adaptorsDesc = AdaptorDescription.merge(projectAdaptorsDesc, resourceAdaptorsDesc);
        HashMap<String, String> properties = ProjectManager.getWorkerProperties(name);
        HashMap<String, String> sharedDisks = new HashMap<String, String>();

        for (int j = 0; j < n.getChildNodes().getLength(); j++) {
            if (n.getChildNodes().item(j).getNodeName().compareTo("Disks") == 0) {
                Node disks = n.getChildNodes().item(j);
                for (int k = 0; k < disks.getChildNodes().getLength(); k++) {
                    if (disks.getChildNodes().item(k).getNodeName().compareTo("Disk") == 0) {
                        Node disk = disks.getChildNodes().item(k);
                        String diskName = disk.getAttributes().getNamedItem("Name").getTextContent();
                        String diskMountpoint = "";
                        for (int ki = 0; ki < disk.getChildNodes().getLength(); ki++) {

                            if (disk.getChildNodes().item(ki).getNodeName().compareTo("MountPoint") == 0) {
                                diskMountpoint = disk.getChildNodes().item(ki).getTextContent();
                            }
                        }
                        sharedDisks.put(diskName, diskMountpoint);
                    }
                }
            }
        }
        new Thread(new WorkerStarter(name, rd, properties, sharedDisks, adaptorsDesc)).start();

    }

    public static TreeMap<String, AdaptorDescription> parseAdaptors(Node n) {
        TreeMap<String, AdaptorDescription> adaptorsDesc = new TreeMap<String, AdaptorDescription>();
        for (int i = 0; i < n.getChildNodes().getLength(); i++) {
            Node property = n.getChildNodes().item(i);
            if (("Adaptors").equals(property.getNodeName())) {
                for (int j = 0; j < property.getChildNodes().getLength(); ++j) {
                    Node adaptor = property.getChildNodes().item(j);
                    if (("Adaptor").equals(adaptor.getNodeName())) {
                        int minPort = 0;
                        int maxPort = -1;
                        String brokerAdaptor = null;

                        String adaptorName = adaptor.getAttributes().getNamedItem("name").getTextContent();
                        for (int k = 0; k < adaptor.getChildNodes().getLength(); k++) {
                            switch (adaptor.getChildNodes().item(k).getNodeName()) {
                                case AdaptorDescription.MAX_PORT:
                                    maxPort = Integer.parseInt(adaptor.getChildNodes().item(k).getTextContent());
                                    break;
                                case AdaptorDescription.MIN_PORT:
                                    minPort = Integer.parseInt(adaptor.getChildNodes().item(k).getTextContent());
                                    break;
                                case AdaptorDescription.BROKER_ADAPTOR:
                                    brokerAdaptor = adaptor.getChildNodes().item(k).getTextContent();
                                    break;
                            }
                        }
                        runtimeLogger.debug("Adding adaptor description: " + adaptorName);
                        adaptorsDesc.put(adaptorName, new AdaptorDescription(adaptorName, minPort, maxPort, brokerAdaptor));
                    }
                }
            }
        }
        return adaptorsDesc;
    }

    private static void loadService(String name, Node n) throws Exception {
        String serviceName = "";
        String namespace = "";
        String portName = "";
        for (int j = 0; j < n.getChildNodes().getLength(); j++) {
            if (n.getChildNodes().item(j).getNodeName().compareTo("Name") == 0) {
                serviceName = n.getChildNodes().item(j).getTextContent();
            }
            if (n.getChildNodes().item(j).getNodeName().compareTo("Namespace") == 0) {
                namespace = n.getChildNodes().item(j).getTextContent();
            }
            if (n.getChildNodes().item(j).getNodeName().compareTo("Port") == 0) {
                portName = n.getChildNodes().item(j).getTextContent();
            }
        }
        HashMap<String, String> properties = ProjectManager.getWorkerProperties(name);
        String taskCountStr = properties.get(ITConstants.LIMIT_OF_TASKS);
        int taskCount = 0;
        if (taskCountStr != null) {
            taskCount = Integer.parseInt(taskCountStr);
        }
        if (taskCount > 0) {
            HashMap<String, String> wsProperties = new HashMap<String, String>();
            wsProperties.put("wsdl", name);
            wsProperties.put("name", serviceName);
            wsProperties.put("namespace", namespace);
            wsProperties.put("port", portName);
            ServiceResourceDescription description = new ServiceResourceDescription(serviceName, namespace, portName);
            Worker<?> newResource = new ServiceWorker(name, description, wsProperties, taskCount);
            addStaticWorker(newResource);
        }
    }

    private static void loadCloudProvider(String name, Node n) throws Exception {
        String connectorPath = "";
        runtimeLogger.info("Loading Provider " + name);
        HashMap<String, String> h = new HashMap<String, String>();
        h.put("estimated-creation-time", "60"); //In seconds (default)
        HashMap<String, String> properties = ProjectManager.getCloudProviderProperties(name);
        for (Entry<String, String> e : properties.entrySet()) {
            h.put(e.getKey(), e.getValue());
        }

        LinkedList<CloudImageDescription> images = new LinkedList<CloudImageDescription>();
        LinkedList<CloudMethodResourceDescription> instanceTypes = new LinkedList<CloudMethodResourceDescription>();
        for (int ki = 0; ki < n.getChildNodes().getLength(); ki++) {
            if (n.getChildNodes().item(ki).getNodeName().compareTo("#text") == 0) {
            } else if (n.getChildNodes().item(ki).getNodeName().compareTo("Connector") == 0) {
                connectorPath = n.getChildNodes().item(ki).getTextContent();
            } else if (n.getChildNodes().item(ki).getNodeName().compareTo("ImageList") == 0) {
                Node imageList = n.getChildNodes().item(ki);
                for (int image = 0; image < imageList.getChildNodes().getLength(); image++) {
                    Node resourcesImageNode = imageList.getChildNodes().item(image);
                    if (resourcesImageNode.getNodeName().compareTo("Image") == 0) {
                        String imageName = resourcesImageNode.getAttributes().getNamedItem("name").getTextContent();
                        Node projectImageNode = ProjectManager.existsImageOnProvider(name, imageName);
                        if (projectImageNode != null) {
                            CloudImageDescription cid = new CloudImageDescription(name, resourcesImageNode, projectImageNode, h);
                            resourcesLogger.info("CLOUD_IMAGE_DESCRIPTION = [\n" + cid.toString("\t") + "]");
                            images.add(cid);
                        } else {
                            runtimeLogger.debug("Image " + name + " not found in project.xml");
                        }

                    }
                }
            } else if (n.getChildNodes().item(ki).getNodeName().compareTo("InstanceTypes") == 0) {
                Node instanceTypesList = n.getChildNodes().item(ki);
                for (int image = 0; image < instanceTypesList.getChildNodes().getLength(); image++) {
                    Node resourcesInstanceTypeNode = instanceTypesList.getChildNodes().item(image);
                    if (resourcesInstanceTypeNode.getNodeName().compareTo("Resource") == 0) {
                        String instanceCode = resourcesInstanceTypeNode.getAttributes().getNamedItem("Name").getTextContent();
                        Node projectTypeNode = ProjectManager.existsInstanceTypeOnProvider(name, instanceCode);
                        if (projectTypeNode != null) {
                            CloudMethodResourceDescription rd = new CloudMethodResourceDescription(resourcesInstanceTypeNode);
                            instanceTypes.add(rd);
                        } else {
                            runtimeLogger.debug("InstanceType " + name + " not found in project.xml");
                        }
                    }
                }
            } else if (n.getChildNodes().item(ki).getNodeName().compareTo("CreationTime") == 0) {
                h.remove("estimated-creation-time");
            } else {
                h.put(n.getChildNodes().item(ki).getNodeName(), n.getChildNodes().item(ki).getTextContent());
            }
        }
        runtimeLogger.debug("Adding Provider " + name);
        CloudManager.newCloudProvider(name, ProjectManager.getCloudProviderLimitOfVMs(name), connectorPath, h);
        try {
            for (CloudImageDescription cid : images) {
                CloudManager.addImageToProvider(name, cid);
            }
            for (CloudMethodResourceDescription instance : instanceTypes) {
                CloudManager.addInstanceTypeToProvider(name, instance);
            }
        } catch (Exception e) {
            /* will never be thrown here, we just added the provider */
        }
        CloudManager.setUseCloud(true);
    }

    public static void coreElementUpdates(LinkedList<Integer> updatedCores) {
        synchronized (pool) {
            pool.coreElementUpdates(updatedCores);
            CloudManager.newCoreElementsDetected(updatedCores);
        }
    }

    public static void addStaticWorker(Worker<?> worker) {
        synchronized (pool) {
            worker.updatedFeatures();
            pool.addStaticResource(worker);
            pool.defineCriticalSet();

            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
        }
        resourceUser.updatedResource(worker);

        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        if (worker.getType() == Resource.Type.SERVICE) {
            runtimeLogger.info("New service available in the pool. Name = " + worker.getName());
        } else {
            runtimeLogger.info("New resource available in the pool. Name = " + worker.getName());
        }
    }

    public static void addCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker) {
        synchronized (pool) {
            CloudManager.confirmedRequest(origin, worker);
            worker.updatedFeatures();
            pool.addDynamicResource(worker);
            pool.defineCriticalSet();

            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
        }

        resourceUser.updatedResource(worker);

        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("New resource available in the pool. Name = " + worker.getName());
    }

    public static void increasedCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker, CloudMethodResourceDescription extension) {
        synchronized (pool) {
            CloudManager.confirmedRequest(origin, worker);
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            worker.increaseFeatures(extension);
            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
        }
        resourceUser.updatedResource(worker);

        // Log modified resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource modified. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource modified. Name = " + worker.getName());
    }

    public static Semaphore reduceCloudWorker(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        Semaphore sem;
        synchronized (pool) {
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            sem = worker.reduceFeatures(reduction);
            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
        }
        resourceUser.updatedResource(worker);

        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource removed from the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource removed from the pool. Name = " + worker.getName());

        return sem;
    }

    public static int[] getTotalSlots() {
        int[] counts = new int[CoreManager.getCoreCount()];
        int[] starterCount = WorkerStarter.getExpectedCoreCount();
        int[] cloudCount = CloudManager.getPendingCoreCounts();
        for (int i = 0; i < counts.length; i++) {
            counts[i] = poolCoreMaxConcurrentTasks[i] + starterCount[i] + cloudCount[i];
        }
        return counts;
    }

    public static int[] getAvailableSlots() {
        return poolCoreMaxConcurrentTasks;
    }

    public static LinkedList<Worker> getAllWorkers() {
        return pool.findAllResources();
    }

    public static Worker getWorker(String name) {
        return pool.getResource(name);
    }

    public static Collection<Worker<?>> getStaticResources() {
        return pool.getStaticResources();
    }

    public static LinkedList<CloudMethodWorker> getDynamicResources() {
        return pool.getDynamicResources();
    }

    public static CloudMethodWorker getDynamicResource(String name) {
        return pool.getDynamicResource(name);
    }

    public static Collection<CloudMethodWorker> getCriticalDynamicResources() {
        return pool.getCriticalResources();
    }

    public static Collection<CloudMethodWorker> getNonCriticalDynamicResources() {
        return pool.getNonCriticalResources();
    }

    public static void refuseCloudRequest(ResourceCreationRequest rcr) {
        CloudManager.refusedRequest(rcr);
    }

    public static ResourcesState getResourcesState() {
        ResourcesState state = new ResourcesState();

        // Set resources information
        for (Worker<?> resource : pool.findAllResources()) {
            if (resource.getType().equals(Type.WORKER)) {
                int cores = ((MethodResourceDescription) resource.getDescription()).getProcessorCoreCount();
                float memory = ((MethodResourceDescription) resource.getDescription()).getMemoryPhysicalSize();
                // Last boolean equals true because this resource is active
                state.addHost(resource.getName(), resource.getType().toString(), cores, memory, resource.getSimultaneousTasks(), true);
            } else {
                // Services doesn't have cores/memory
                // Last boolean equals true because this resource is active
                state.addHost(resource.getName(), resource.getType().toString(), 0, (float) 0.0, resource.getSimultaneousTasks(), true);
            }
        }

        // Set cloud information
        state.setUseCloud(CloudManager.isUseCloud());
        if (state.getUseCloud()) {
            try {
                state.setCreationTime(CloudManager.getNextCreationTime());
            } catch (Exception ex) {
                state.setCreationTime(120000l);
            }
            state.setCurrentCloudVMCount(CloudManager.getCurrentVMCount());

            for (ResourceCreationRequest rcr : CloudManager.getPendingRequests()) {
                int[][] simTasks = rcr.requestedSimultaneousTaskCount();
                for (int coreId = 0; coreId < simTasks.length; coreId++) {
                    int coreSlots = 0;
                    for (int implId = 0; implId < simTasks[coreId].length; ++implId) {
                        coreSlots += Math.max(coreSlots, simTasks[coreId][implId]);
                    }
                    // Last boolean equals false because this resource is pending
                    state.updateHostInfo(rcr.getRequested().getName(), rcr.getRequested().getType(),
                            rcr.getRequested().getProcessorCoreCount(), rcr.getRequested().getMemoryPhysicalSize(),
                            coreId, coreSlots, false);
                }
            }
        }

        return state;
    }

    public static String getPendingRequestsMonitorData(String prefix) {
        StringBuilder sb = new StringBuilder();
        LinkedList<ResourceCreationRequest> rcr = CloudManager.getPendingRequests();
        for (ResourceCreationRequest r : rcr) {
            sb.append(prefix).append("<Resource id=\"" + r.getRequested().getName() + "\">").append("\n");
            sb.append(prefix + "\t").append("<CPU>").append(r.getRequested().getProcessorCPUCount()).append("</CPU>").append("\n");
            sb.append(prefix + "\t").append("<Core>").append(r.getRequested().getProcessorCoreCount()).append("</Core>").append("\n");
            sb.append(prefix + "\t").append("<Memory>").append(r.getRequested().getMemoryPhysicalSize()).append("</Memory>").append("\n");
            sb.append(prefix + "\t").append("<Disk>").append(r.getRequested().getStorageElemSize()).append("</Disk>").append("\n");
            sb.append(prefix + "\t").append("<Provider>").append(r.getProvider()).append("</Provider>").append("\n");
            sb.append(prefix + "\t").append("<Image>").append(r.getRequested().getImage()).append("</Image>").append("\n");
            sb.append(prefix + "\t").append("<Status>").append("Creating").append("</Status>").append("\n");
            sb.append(prefix + "\t").append("<Tasks>").append("</Tasks>").append("\n");
            sb.append(prefix).append("</Resource>").append("\n");
        }
        return sb.toString();
    }

    public static void printLoadInfo() {
        resourcesLogger.info(resourceUser.getWorkload().toString());
    }

    public static void printResourcesState() {
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info(getResourcesState().toString());
    }

    public static Long getCreationTime()
            throws Exception {
        try {
            return CloudManager.getNextCreationTime();
        } catch (ConnectorException e) {
            throw new Exception(e);
        }
    }

    // Stop all the nodes: vm
    public static void stopNodes(WorkloadStatus status) {
        // Log resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Stopping all workers]");
        runtimeLogger.info("Stopping all workers");
        
        if (ro != null) {
            ro.shutdown(status);
        } else {
            runtimeLogger.info("Resource Optimizer was not initialized");
        }

        // Stop all Cloud VM
        if (CloudManager.isUseCloud()) {
            // Transfer files 
        	/*Semaphore sem = new Semaphore(0);
             ShutdownListener sl = new ShutdownListener(sem);
             resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping cloud workers...]");
             for (Worker<?,?> r : pool.getDynamicResources()) {
             // TODO: The worker is not really needed to be stopped because VM is going to be erased.
             //       However, the app-files and the tracing files MUST be transfered
             r.stop(false, sl);
             }
             resourcesLogger.debug("DEBUG_MSG = [Waiting for cloud workers to shutdown...]");
             sl.enable();
             try {
             sem.acquire();
             } catch (Exception e) {
             resourcesLogger.error("ERROR_MSG= [ERROR: Exception raised on cloud worker shutdown]");
             }
             resourcesLogger.info("INFO_MSG = [Cloud Workers stopped]");
             */
            resourcesLogger.debug("DEBUG_MSG = [Terminating cloud instances...]");
            try {
                CloudManager.terminateALL();
                resourcesLogger.info("TOTAL_EXEC_COST = " + CloudManager.getTotalCost());
            } catch (Exception e) {
                resourcesLogger.error(ITConstants.TS + ": " + DEL_VM_ERR, e);
            }
            resourcesLogger.info("INFO_MSG = [Cloud instances terminated]");
        }

        // Stop static workers - Order its destruction from runtime and transfer files
        // Physical worker (COMM) is erased now - because of cloud
        if (pool != null && !pool.getStaticResources().isEmpty()) {

            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping workers...]");
            for (Worker<?> r : pool.getStaticResources()) {
                r.stop(false, sl);
            }

            resourcesLogger.debug("DEBUG_MSG = [Waiting for workers to shutdown...]");
            sl.enable();

            try {
                sem.acquire();
            } catch (Exception e) {
                resourcesLogger.error("ERROR_MSG= [ERROR: Exception raised on worker shutdown]");
            }
            resourcesLogger.info("INFO_MSG = [Workers stopped]");
        }
    }

    public static boolean useCloud() {
        return CloudManager.isUseCloud();
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool
     *
     * @return the cost per hour of the whole pool
     */
    public static float getCurrentCostPerHour() {
        return CloudManager.currentCostPerHour();
    }

    /**
     * The CloudManager computes the accumulated cost of the execution
     *
     * @return cost of the whole execution
     */
    public static float getTotalCost() {
        return CloudManager.getTotalCost();
    }

    public static String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");
        sb.append(pool.getCurrentState(prefix)).append("\n");
        sb.append(CloudManager.getCurrentState(prefix));
        return sb.toString();

    }
}
