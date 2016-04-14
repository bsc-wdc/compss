package integratedtoolkit.util;

import java.util.List;
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
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.ResourcesState;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.ServiceResourceDescription;
import integratedtoolkit.types.resources.ServiceWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
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
     * @throws Exception Parsing the xml file or creating new instances for the
     * Cloud providers connectors
     */
    public static void load(ResourceUser resUser) throws Exception {
        pool = new WorkerPool(CoreManager.getCoreCount());
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
            runtimeLogger.error("Error Validating resources.xml config file.\n",e);
        	System.err.println("Error Validating resources.xml config file.\n" + e.getMessage());
        } catch (IOException e){
        	runtimeLogger.error("IOError while trying to validate resources.xml config file.\n",e);
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
                try{
                    loadService(name, n);
                } catch (Exception e){
                	resourcesLogger.error("Could not load service " + name);
                }
            } else if (n.getNodeName().compareTo("CloudProvider") == 0) {
                String cloudProviderName = n.getAttributes().getNamedItem("name").getTextContent();
                if (!ProjectManager.existsCloudProvider(cloudProviderName)) {
                    runtimeLogger.debug("Provider "+ cloudProviderName + " not in project.xml");
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
            if (("Adaptors").equals(property.getNodeName())){
            	for (int j = 0; j < property.getChildNodes().getLength(); ++j){
                    Node adaptor = property.getChildNodes().item(j);
                    if(("Adaptor").equals(adaptor.getNodeName())){
                        int minPort = 0;
                        int maxPort = -1;
                        String brokerAdaptor = null;
                        
                        String adaptorName = adaptor.getAttributes().getNamedItem("name").getTextContent();
                        for (int k = 0; k < adaptor.getChildNodes().getLength(); k++) {
                            switch(adaptor.getChildNodes().item(k).getNodeName()){
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
                        runtimeLogger.debug("Adding adaptor description: "+ adaptorName);
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
            newResource.updatedFeatures();
            
            synchronized (pool) {
                //pool.addStaticResource(newResource);
                pool.addResourceLinks(newResource);
                pool.defineCriticalSet();
            }
            resourceUser.createdResources(newResource);
            
            // Log new resource
            resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
            resourcesLogger.info("INFO_MSG = [New service available. Name = " + newResource.getName() + "]");
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
                        }else{
                        	runtimeLogger.debug("Image "+ name + " not found in project.xml");
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
                        }else{
                        	runtimeLogger.debug("InstanceType "+ name + " not found in project.xml");
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

    public static Worker<?> getWorker(String name) {
        return pool.getResource(name);
    }

    /**
     * Return a list of all the resources
     *
     * @return list of all the resources
     */
    public static List<Worker<?>> getAllWorkers() {
        return pool.findAllResources();
    }

    public static void addStaticWorker(Worker<?> worker) {
        worker.updatedFeatures();
        synchronized (pool) {
            pool.addStaticResource(worker);
            pool.addResourceLinks(worker);
        }
        resourceUser.createdResources(worker);
        
        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("New resource available in the pool. Name = " + worker.getName());
    }

    public static void addCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker) {
        synchronized (pool) {
            pool.addDynamicResource(worker);
            CloudManager.confirmedRequest(origin, worker);
        }
        worker.updatedFeatures();
        synchronized (pool) {
            pool.addResourceLinks(worker);
            pool.defineCriticalSet();
        }

        resourceUser.createdResources(worker);
        
        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("New resource available in the pool. Name = " + worker.getName());
    }

    public static void increasedCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker, CloudMethodResourceDescription extension) {
        synchronized (pool) {
            pool.removeResourceLinks(worker);
            CloudManager.confirmedRequest(origin, worker);
            worker.increaseFeatures(extension);
            pool.addResourceLinks(worker);
            pool.defineCriticalSet();
        }
        resourceUser.createdResources(worker);
        
        // Log modified resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource modified. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource modified. Name = " + worker.getName());
    }

    public static Semaphore reduceCloudWorker(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        Semaphore sem;
        synchronized (pool) {
            pool.removeResourceLinks(worker);
            sem = worker.reduceFeatures(reduction);
            pool.addResourceLinks(worker);
            pool.defineCriticalSet();
        }
        resourceUser.createdResources(worker);
        
        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource removed from the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource removed from the pool. Name = " + worker.getName());
        
        return sem;
    }

    public static int[] getTotalSlots() {
        int[] counts = new int[CoreManager.getCoreCount()];
        int[] poolCounts = pool.getCoreMaxTaskCount();
        int[] starterCount = WorkerStarter.getExpectedCoreCount();
        int[] cloudCount = CloudManager.getPendingCoreCounts();
        for (int i = 0; i < counts.length; i++) {
            counts[i] = poolCounts[i] + starterCount[i] + cloudCount[i];
        }
        return counts;
    }

    public static int[] getAvailableSlots() {
        return pool.getCoreMaxTaskCount();
    }

    public static Collection<Worker<?>> getStaticResources() {
        return pool.getStaticResources();
    }

    public static LinkedList<CloudMethodWorker> getDynamicResources() {
        return pool.getDynamicResources();
    }

    public static Collection<CloudMethodWorker> getCriticalDynamicResources() {
        return pool.getCriticalResources();
    }

    public static Collection<CloudMethodWorker> getNonCriticalDynamicResources() {
        return pool.getNonCriticalResources();
    }

    public static CloudMethodWorker getDynamicResource(String name) {
        return pool.getDynamicResource(name);
    }

    public static void refuseCloudRequest(ResourceCreationRequest rcr) {
        CloudManager.refusedRequest(rcr);
    }

    /**
     * Return a list of all the resources able to run a task of the core coreId
     *
     * @param coreId Id of the task's core
     * @return list of all the resources able to run a task of the core coreId
     */
    public static LinkedList<Worker<?>> findCompatibleWorkers(int coreId) {
        return pool.findCompatibleResources(coreId);
    }

    public static HashMap<Worker<?>, LinkedList<Implementation<?>>> findAvailableWorkers(LinkedList<Worker<?>> compatibleWorkers, int coreId) {
        HashMap<Worker<?>, LinkedList<Implementation<?>>> available = new HashMap<Worker<?>, LinkedList<Implementation<?>>>();
        for (Worker r : compatibleWorkers) {
            LinkedList<Implementation<?>> availableImpls = new LinkedList<Implementation<?>>();
            LinkedList<Implementation<?>> compatibleImpls = r.getExecutableImpls(coreId);
            for (Implementation<?> compatibleImpl : compatibleImpls) {
                if (r.canRunNow(compatibleImpl.getRequirements())) {
                    availableImpls.add(compatibleImpl);
                }
            }
            if (!availableImpls.isEmpty()) {
                available.put(r, availableImpls);
            }
        }
        return available;
    }

    public static ResourcesState getResourcesState() {
        ResourcesState state = new ResourcesState();
        
        // Set resources information
        for (Worker<?> resource : ResourceManager.getAllWorkers()) {
        	if (resource.getType().equals(Type.WORKER)) {
        		int cores = ((MethodResourceDescription)resource.getDescription()).getProcessorCoreCount();
        		float memory = ((MethodResourceDescription)resource.getDescription()).getMemoryPhysicalSize();
        		// Last boolean equals true because this resource is active
        		state.addHost(resource.getName(), resource.getType().toString(), cores, memory, resource.getSimultaneousTasks(), true);
        	} else {
        		// Services doesn't have cores/memory
        		// Last boolean equals true because this resource is active
        		state.addHost(resource.getName(), resource.getType().toString(), 0, (float)0.0, resource.getSimultaneousTasks(), true);
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
        if (ro!=null){
        	ro.shutdown(status);
        }else{
        	runtimeLogger.info("Resource Optimizer was not initialized");
        }
        
        // Stop all Cloud VM
        if (CloudManager.isUseCloud()) {
        	// Transfer files 
        	/*Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping cloud workers...]");
            for (Worker<?> r : pool.getDynamicResources()) {
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
        if(pool!=null && !pool.getStaticResources().isEmpty()) {
        	
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
    /*
     private static void manageResourceDestruction(CloudMethodWorker res, ResourceDestructionRequest rdr, int[][] slotReduction) {
     logger.info("ORDER_DESTRUCTION = [\n\tTYPE = " + rdr.getRequested().getType() + "\n\tPROVIDER = " + rdr.getRequested().getName() + "\n]");
     if (debug) {
     StringBuilder sb = new StringBuilder();
     sb.append("EXPECTED_SIM_TASKS = [").append("\n");
     for (int i = 0; i < CoreManager.coreCount; i++) {
     for (int j = 1; j < CoreManager.getCoreImplementations(i).length; ++j) {
     sb.append("\t").append("IMPLEMENTATION_INFO = [").append("\n");
     sb.append("\t").append("\t").append("COREID = ").append(i).append("\n");
     sb.append("\t").append("\t").append("IMPLID = ").append(j).append("\n");
     sb.append("\t").append("\t").append("SIM_TASKS = ").append(slotReduction[i][j]).append("\n");
     sb.append("\t").append("]").append("\n");
     }
     }
     sb.append("]");
     logger.debug(sb.toString());
     }
     //Fa la reserva i fa al pool si hi ha prous recursos pendents
     if (pool.markResourcesToRemove(res, rdr.getRequested(), slotReduction)) {
     //Afegeix la destrucció a la màquina i indica si es destrueix totalment
     CloudManager.performReduction(rdr);
     if (!rdr.isTerminate()) {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere are enough available resources to be destroyed and the VM is not completely killed. Terminating resource.\n]");
     }
     CloudManager.terminate(rdr);
     } else {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere are enough available resources to be destroyed and the VM is completely killed. All the unique data must be saved.\n]");
     }
     pool.delete(res);
     }
     } else {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere are not enough available resources to perform the modification. Registering modification as pending.\n]");
     }
     LinkedList<ResourceDestructionRequest> modifications = host2PendingModifications.get(res);
     if (modifications == null) {
     modifications = new LinkedList<ResourceDestructionRequest>();
     host2PendingModifications.put(res, modifications);
     }
     CloudManager.addPendingReduction(rdr);
     modifications.add(rdr);
     }
     }

     public static ResourceDestructionRequest checkPendingModifications(CloudMethodWorker worker) {
     LinkedList<ResourceDestructionRequest> modifications = host2PendingModifications.get(worker);
     if (modifications != null && !modifications.isEmpty()) { //if there are pending modifications
     CloudMethodWorker res = (CloudMethodWorker) worker;
     ResourceDestructionRequest modification = modifications.get(0);
     if (res.hasAvailable(modification.getRequested())) {
     modifications.removeFirst();
     pool.confirmPendingReduction(res, modification.getRequested());
     CloudManager.confirmReduction(modification);
     if (!modification.isTerminate()) {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere is a pending modification and enough available resources to be destroyed. The vm will be alive after performing the modification so the resources are destroyed.\n]");
     }
     CloudManager.terminate(modification);
     } else {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere is a pending modification and enough available resources to be destroyed. The vm will be completely killed and unique data must be saved.\n]");
     }
     pool.delete(res);
     }
     return modification;
     } else {
     if (debug) {
     logger.debug("DEBUG_MSG = [\n\tThere is a pending modification for node " + res.getName() + ", but there are not enough available resources yet.\n]");

     }
     }
     }
     return null;
     }

     //Removes a resource from the pool if its an useless noncritical cloud resource
     public static LinkedList<ResourceDestructionRequest> tryToTerminate(String resourceName, int[] counts) {
     CloudMethodWorker res = null;
     if (CloudManager.isUseCloud() && (res = pool.getDynamicResource(resourceName)) != null) {
     boolean useful = checkResourceUsefulness(res, counts);
     if (useful) {
     return new LinkedList<ResourceDestructionRequest>();
     }
     return destroyUselessResource(res);
     }
     return new LinkedList<ResourceDestructionRequest>();
     }

     public static LinkedList<ResourceDestructionRequest> terminateUnbounded(int[] counts) {
     LinkedList<ResourceDestructionRequest> rdrs = new LinkedList<ResourceDestructionRequest>();
     CloudManager.stopReached();
     if (CloudManager.isUseCloud()) {
     LinkedList<CloudMethodWorker> resources = pool.getDynamicResources();
     for (CloudMethodWorker candidate : resources) {
     boolean useful = checkResourceUsefulness(candidate, counts);
     if (useful) {
     continue;
     }
     rdrs.addAll(destroyUselessResource(candidate));
     }
     }
     return rdrs;
     }

     private static boolean checkResourceUsefulness(CloudMethodWorker res, int[] counts) {
     boolean useful = res.getTaskCount() > 0;
     LinkedList<Integer> executableCores = res.getExecutableCores();
     int[] simTasks = res.getSimultaneousTasks();
     int[] totalSimTasks = pool.getCoreMaxTaskCount();
     for (int i = 0; i < executableCores.size() && !useful; i++) {
     int coreId = executableCores.get(i);
     useful = counts[coreId] > (totalSimTasks[coreId] - simTasks[coreId]);
     }
     return useful;
     }

     private static LinkedList<ResourceDestructionRequest> destroyUselessResource(CloudMethodWorker res) {
     LinkedList<ResourceDestructionRequest> deletions = new LinkedList<ResourceDestructionRequest>();
     pool.delete(res);
     HashMap<CloudMethodResourceDescription, Integer> composition = CloudManager.getVMComposition(res.getName());
     for (java.util.Map.Entry<CloudMethodResourceDescription, Integer> entry : composition.entrySet()) {
     CloudMethodResourceDescription type = entry.getKey();
     Integer amount = entry.getValue();
     for (int i = 0; i < amount; i++) {
     CloudMethodResourceDescription rd = new CloudMethodResourceDescription(type);
     rd.setName(res.getName());
     ResourceDestructionRequest rdr = new ResourceDestructionRequest(rd, false);
     CloudManager.performReduction(rdr);
     if (!rdr.isTerminate()) {
     CloudManager.terminate(rdr);
     }
     deletions.add(rdr);
     }
     }
     return deletions;
     }

     public static ResourceDestructionRequest reduceResources(float[] destroyRecommendations, boolean mandatoryDestruction) {
     //Getting all modifiable resources
     java.util.Set<CloudMethodWorker> noncritical = pool.getNonCriticalResources(destroyRecommendations, mandatoryDestruction);
     java.util.Set<CloudMethodWorker> critical = pool.getCriticalResources(destroyRecommendations, mandatoryDestruction);
     //Getting best destruction option for each set
     Object[] noncriticalSolution = CloudManager.getBestDestruction(noncritical, destroyRecommendations);
     Object[] criticalSolution = CloudManager.getBestDestruction(critical, destroyRecommendations);

     boolean criticalIsBetter;
     if (criticalSolution == null) {
     if (noncriticalSolution == null) {
     return null;
     } else {
     criticalIsBetter = false;
     }
     } else {
     if (noncriticalSolution == null) {
     criticalIsBetter = true;
     } else {
     criticalIsBetter = false;
     float[] noncriticalValues = (float[]) noncriticalSolution[1];
     float[] criticalValues = (float[]) criticalSolution[1];

     if (noncriticalValues[0] == criticalValues[0]) {
     if (noncriticalValues[1] == criticalValues[1]) {
     if (noncriticalValues[2] < criticalValues[2]) {
     criticalIsBetter = true;
     }
     } else {
     if (noncriticalValues[1] > criticalValues[1]) {
     criticalIsBetter = true;
     }
     }
     } else {
     if (noncriticalValues[0] > criticalValues[0]) {
     criticalIsBetter = true;
     }
     }
     }

     }

     CloudMethodWorker res;
     float[] record;
     CloudMethodResourceDescription rd;
     int[][] slotsRemovingCount;
     if (criticalIsBetter && pool.isCriticalRemovalSafe((int[][]) criticalSolution[2])) {
     res = (CloudMethodWorker) criticalSolution[0];
     record = (float[]) criticalSolution[1];
     slotsRemovingCount = (int[][]) criticalSolution[2];
     rd = (CloudMethodResourceDescription) criticalSolution[3];
     } else {
     if (noncriticalSolution == null) {
     return null;
     }
     res = (CloudMethodWorker) noncriticalSolution[0];
     record = (float[]) noncriticalSolution[1];
     slotsRemovingCount = (int[][]) noncriticalSolution[2];
     rd = (CloudMethodResourceDescription) noncriticalSolution[3];
     }

     if (!mandatoryDestruction && record[1] > 0) {
     return null;
     } else {
     CloudMethodResourceDescription finalDescription = new CloudMethodResourceDescription(rd);
     finalDescription.setName(res.getName());
     ResourceDestructionRequest rdr = new ResourceDestructionRequest(finalDescription, false);
     manageResourceDestruction(res, rdr, slotsRemovingCount);
     return rdr;
     }
     }
     */

}
