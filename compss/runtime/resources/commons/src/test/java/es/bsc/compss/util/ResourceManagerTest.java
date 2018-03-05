package es.bsc.compss.util;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.fake.FakeMethodAdaptor;
import es.bsc.compss.comm.fake.FakeServiceAdaptor;
import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ExtendedCloudMethodWorker;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.fake.FakeNode;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.MethodWorker;
import es.bsc.compss.types.resources.ServiceResourceDescription;
import es.bsc.compss.types.resources.ServiceWorker;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.configuration.ServiceConfiguration;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.resources.updates.PendingReduction;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class ResourceManagerTest {

    public static final String RUNTIME_CONNECTOR = "es.bsc.compss.connectors.fake.FakeConnector";

    private ResourceUser rus = new ResourceUser() {

        @Override
        public <T extends WorkerResourceDescription> void updatedResource(Worker<T> r, ResourceUpdate<T> modification) {

        }
    };


    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        ResourceManager.clear(rus);
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testStaticWorkersOperations() {
        // Add Method Resource 1
        MethodResourceDescription mrd1 = new MethodResourceDescription();
        Float memory1 = 10f + (float) Math.random();
        mrd1.setMemorySize(memory1);
        MethodConfiguration mc1 = null;
        try {
            mc1 = (MethodConfiguration) Comm.constructConfiguration(FakeMethodAdaptor.class.getName(), null, null);
        } catch (Exception e) {
        }
        String worker1Name = "Worker" + (int) (Math.random() * 10000);
        MethodWorker mw1 = createMethodWorker(worker1Name, mrd1, new HashMap<>(), mc1);
        ResourceManager.addStaticResource(mw1);

        if (ResourceManager.getTotalNumberOfWorkers() != 1) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getStaticResources().contains(mw1)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(worker1Name) != mw1) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getWorker(worker1Name).getName().equals(worker1Name)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (((MethodResourceDescription) ResourceManager.getWorker(worker1Name).getDescription()).getMemorySize() != mrd1.getMemorySize()) {
            fail("ResourceManager is not properly adding Method Workers");
        }

        // Add Method Resource 2
        MethodResourceDescription mrd2 = new MethodResourceDescription();
        Float memory2 = 10f + (float) Math.random();
        mrd2.setMemorySize(memory2);
        MethodConfiguration mc2 = null;
        try {
            mc2 = (MethodConfiguration) Comm.constructConfiguration(FakeMethodAdaptor.class.getName(), null, null);
        } catch (Exception e) {
        }
        String worker2Name = "Worker" + (int) (Math.random() * 10000);
        MethodWorker mw2 = createMethodWorker(worker2Name, mrd2, new HashMap<>(), mc2);
        ResourceManager.addStaticResource(mw2);
        if (ResourceManager.getTotalNumberOfWorkers() != 2) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getStaticResources().contains(mw2)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(worker2Name) != mw2) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getWorker(worker2Name).getName().equals(worker2Name)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (((MethodResourceDescription) ResourceManager.getWorker(worker2Name).getDescription()).getMemorySize() != mrd2.getMemorySize()) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        // Add Service Resource 1
        ServiceResourceDescription srd1 = new ServiceResourceDescription("service1", "namespace1", "port1", 2);
        ServiceConfiguration sc1 = null;
        try {
            sc1 = (ServiceConfiguration) Comm.constructConfiguration(FakeServiceAdaptor.class.getName(), null, null);
        } catch (Exception e) {
        }
        String wsdl1 = "WSDL" + (int) (Math.random() * 10000);
        ServiceWorker sw1 = createServiceWorker(wsdl1, srd1, sc1);
        ResourceManager.addStaticResource(sw1);
        if (ResourceManager.getTotalNumberOfWorkers() != 3) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getStaticResources().contains(sw1)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(wsdl1) != sw1) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!ResourceManager.getWorker(wsdl1).getName().equals(wsdl1)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (!((ServiceResourceDescription) ResourceManager.getWorker(wsdl1).getDescription()).getNamespace().equals(srd1.getNamespace())) {
            fail("ResourceManager is not properly adding Method Workers");
        }

        ResourceManager.removeWorker(sw1);
        if (ResourceManager.getTotalNumberOfWorkers() != 2) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getStaticResources().contains(sw1)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(wsdl1) != null) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        ResourceManager.removeWorker(mw1);
        if (ResourceManager.getTotalNumberOfWorkers() != 1) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getStaticResources().contains(worker1Name)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(worker1Name) != null) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        ResourceManager.removeWorker(mw2);
        if (ResourceManager.getTotalNumberOfWorkers() != 0) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getStaticResources().contains(worker2Name)) {
            fail("ResourceManager is not properly adding Method Workers");
        }
        if (ResourceManager.getWorker(worker2Name) != null) {
            fail("ResourceManager is not properly adding Method Workers");
        }
    }

    @Test
    public void testCloudConfiguration() {
        if (ResourceManager.useCloud()) {
            fail("ResourceManager has cloud enabled by default");
        }
        ResourceManager.setCloudVMsBoundaries(3, 5, 8);
        if (ResourceManager.getMinCloudVMs() != 3) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getInitialCloudVMs() != 5) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getMaxCloudVMs() != 8) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        ResourceManager.setCloudVMsBoundaries(9, 5, 8);
        if (ResourceManager.getMinCloudVMs() != 9) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getInitialCloudVMs() != 9) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getMaxCloudVMs() != 9) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }

        ResourceManager.setCloudVMsBoundaries(3, null, null);
        if (ResourceManager.getMinCloudVMs() != 3) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getInitialCloudVMs() != 5) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }
        if (ResourceManager.getMaxCloudVMs() != 8) {
            fail("ResourceManager does not properly configure the cloud boundaries");
        }

        CloudProvider cp1 = addProvider();
        if (ResourceManager.getAvailableCloudProviders().size() != 1 || !ResourceManager.getAvailableCloudProviders().contains(cp1)) {
            fail("ResourceManager does not properly register cloud providers");
        }
        if (!ResourceManager.useCloud()) {
            fail("ResourceManager does not detect that the cloud has been enabled");
        }
        CloudProvider cp2 = addProvider();
        if (ResourceManager.getAvailableCloudProviders().size() != 2 || !ResourceManager.getAvailableCloudProviders().contains(cp1)
                || !ResourceManager.getAvailableCloudProviders().contains(cp2)) {
            fail("ResourceManager does not properly register cloud providers");
        }

    }

    @Test
    public void testOneCloudWorkersOperations() {
        ResourceManager.setCloudVMsBoundaries(3, 5, 8);
        CloudProvider cp1 = addProvider();

        CloudMethodResourceDescription cmrd1 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr1 = cp1.requestResourceCreation(cmrd1);

        if (ResourceManager.getPendingCreationRequests().size() != 1 || !ResourceManager.getPendingCreationRequests().contains(rcr1)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        String vmName1 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw1 = new ExtendedCloudMethodWorker(vmName1, cp1, cmrd1, new FakeNode(vmName1), 0, new HashMap<>());
        ResourceManager.addCloudWorker(rcr1, cmw1, cmrd1);
        if (!ResourceManager.getPendingCreationRequests().isEmpty() || ResourceManager.getPendingCreationRequests().contains(rcr1)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName1) != cmw1 || ResourceManager.getWorker(vmName1) != cmw1) {
            fail("ResourceManager is not properly adding the new cloud resources");
        }
        CloudMethodResourceDescription reduction1 = new CloudMethodResourceDescription(cmrd1);
        ResourceManager.reduceResource(cmw1, new PendingReduction<>(reduction1));
        ResourceManager.terminateCloudResource(cmw1, reduction1);
        if (!ResourceManager.getDynamicResources().isEmpty() || !ResourceManager.getAllWorkers().isEmpty()
                || ResourceManager.getDynamicResource(vmName1) != null || ResourceManager.getWorker(vmName1) != null) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }

        if (!cmw1.isTerminated()) {
            fail("ResourceManager is not properly requesting the resource shutdown");
        }
    }

    @Test
    public void testMultipleCloudWorkersOperations() {
        ResourceManager.setCloudVMsBoundaries(3, 5, 8);
        CloudProvider cp1 = addProvider();
        CloudProvider cp2 = addProvider();

        CloudMethodResourceDescription cmrd1 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr1 = cp1.requestResourceCreation(cmrd1);

        CloudMethodResourceDescription cmrd2 = createResourceDescriptionFromProvider(cp2);
        ResourceCreationRequest rcr2 = cp2.requestResourceCreation(cmrd1);

        CloudMethodResourceDescription cmrd3 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr3 = cp1.requestResourceCreation(cmrd3);

        if (ResourceManager.getPendingCreationRequests().size() != 3 || !ResourceManager.getPendingCreationRequests().contains(rcr1)
                || !ResourceManager.getPendingCreationRequests().contains(rcr2)
                || !ResourceManager.getPendingCreationRequests().contains(rcr3)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }

        String vmName1 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw1 = new ExtendedCloudMethodWorker(vmName1, cp1, cmrd1, new FakeNode(vmName1), 0, new HashMap<>());
        ResourceManager.addCloudWorker(rcr1, cmw1, cmrd1);
        if (ResourceManager.getPendingCreationRequests().size() != 2 || ResourceManager.getPendingCreationRequests().contains(rcr1)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName1) != cmw1 || ResourceManager.getWorker(vmName1) != cmw1) {
            fail("ResourceManager is not properly adding the new cloud resources");
        }

        String vmName2 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw2 = new ExtendedCloudMethodWorker(vmName2, cp2, cmrd2, new FakeNode(vmName2), 0, new HashMap<>());
        ResourceManager.addCloudWorker(rcr2, cmw2, cmrd2);
        if (ResourceManager.getPendingCreationRequests().size() != 1 || ResourceManager.getPendingCreationRequests().contains(rcr2)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getDynamicResources().size() != 2 || ResourceManager.getAllWorkers().size() != 2
                || ResourceManager.getDynamicResource(vmName2) != cmw2 || ResourceManager.getWorker(vmName2) != cmw2) {
            fail("ResourceManager is not properly adding the new cloud resources");
        }

        CloudMethodResourceDescription reduction1 = new CloudMethodResourceDescription(cmrd1);
        ResourceManager.reduceResource(cmw1, new PendingReduction<>(reduction1));
        ResourceManager.terminateCloudResource(cmw1, reduction1);
        if (ResourceManager.getPendingCreationRequests().size() != 1) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getCurrentVMCount() != 2) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName1) != null || ResourceManager.getWorker(vmName1) != null) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }

        if (!cmw1.isTerminated()) {
            fail("ResourceManager is not properly requesting the resource shutdown");
        }

        String vmName3 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw3 = new ExtendedCloudMethodWorker(vmName3, cp1, cmrd3, new FakeNode(vmName3), 0, new HashMap<>());
        ResourceManager.addCloudWorker(rcr3, cmw3, cmrd3);
        if (!ResourceManager.getPendingCreationRequests().isEmpty()) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getDynamicResources().size() != 2 || ResourceManager.getAllWorkers().size() != 2
                || ResourceManager.getDynamicResource(vmName3) != cmw3 || ResourceManager.getWorker(vmName3) != cmw3) {
            fail("ResourceManager is not properly adding the new cloud resources");
        }

        CloudMethodResourceDescription reduction2 = new CloudMethodResourceDescription(cmrd2);
        ResourceManager.reduceResource(cmw2, new PendingReduction<>(reduction2));
        ResourceManager.terminateCloudResource(cmw2, reduction2);
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName2) != null || ResourceManager.getWorker(vmName2) != null) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }
        if (!cmw2.isTerminated()) {
            fail("ResourceManager is not properly requesting the resource shutdown");
        }

        if (ResourceManager.getCurrentVMCount() != 1) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }

        CloudMethodResourceDescription cmrd4 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr4 = cp1.requestResourceCreation(cmrd4);
        if (ResourceManager.getPendingCreationRequests().size() != 1 || !ResourceManager.getPendingCreationRequests().contains(rcr4)) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getCurrentVMCount() != 2) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : cmw3.getDescription().getTypeComposition().entrySet()) {
            if (entry.getValue()[0] != 1) {
                fail("ResourceManager is not properly keeping track of the amount of instances of each type");
            }
        }

        ResourceManager.increasedCloudWorker(rcr4, cmw3, cmrd4);
        if (!ResourceManager.getPendingCreationRequests().isEmpty()) {
            fail("ResourceManager is not properly registering the pending resouce creations");
        }
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName3) != cmw3 || ResourceManager.getWorker(vmName3) != cmw3) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }
        if (ResourceManager.getCurrentVMCount() != 2) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : cmw3.getDescription().getTypeComposition().entrySet()) {
            if (entry.getValue()[0] != 2) {
                fail("ResourceManager is not properly keeping track of the amount of instances of each type");
            }
        }

        CloudMethodResourceDescription reduction4 = new CloudMethodResourceDescription(cmrd4);
        ResourceManager.reduceResource(cmw3, new PendingReduction<>(reduction4));
        ResourceManager.terminateCloudResource(cmw3, reduction4);
        if (ResourceManager.getDynamicResources().size() != 1 || ResourceManager.getAllWorkers().size() != 1
                || ResourceManager.getDynamicResource(vmName3) != cmw3 || ResourceManager.getWorker(vmName3) != cmw3) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }
        if (ResourceManager.getCurrentVMCount() != 1) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : cmw3.getDescription().getTypeComposition().entrySet()) {
            if (entry.getValue()[0] != 1) {
                fail("ResourceManager is not properly keeping track of the amount of instances of each type");
            }
        }

        CloudMethodResourceDescription reduction3 = new CloudMethodResourceDescription(cmrd3);
        ResourceManager.reduceResource(cmw3, new PendingReduction<>(reduction3));
        ResourceManager.terminateCloudResource(cmw3, reduction3);
        if (!ResourceManager.getDynamicResources().isEmpty() || !ResourceManager.getAllWorkers().isEmpty()
                || ResourceManager.getDynamicResource(vmName3) != null || ResourceManager.getWorker(vmName3) != null) {
            fail("ResourceManager is not properly removing the new cloud resources");
        }
        if (ResourceManager.getCurrentVMCount() != 0) {
            fail("ResourceManager is not properly keeping track of the created VMs");
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : cmw3.getDescription().getTypeComposition().entrySet()) {
            if (entry.getValue()[0] != 0) {
                fail("ResourceManager is not properly keeping track of the amount of instances of each type");
            }
        }
    }

    private static MethodWorker createMethodWorker(String name, MethodResourceDescription rd, HashMap<String, String> sharedDisks,
            MethodConfiguration mc) {
        // Compute task count
        int taskCount;
        int limitOfTasks = mc.getLimitOfTasks();
        int computingUnits = rd.getTotalCPUComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }

        mc.setLimitOfTasks(taskCount);

        limitOfTasks = mc.getLimitOfGPUTasks();
        computingUnits = rd.getTotalGPUComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfGPUTasks(taskCount);

        limitOfTasks = mc.getLimitOfFPGATasks();
        computingUnits = rd.getTotalFPGAComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfFPGATasks(taskCount);

        limitOfTasks = mc.getLimitOfOTHERSTasks();
        computingUnits = rd.getTotalOTHERComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfOTHERSTasks(taskCount);

        MethodWorker methodWorker = new MethodWorker(name, rd, mc, sharedDisks);
        return methodWorker;
    }

    private static ServiceWorker createServiceWorker(String wsdl, ServiceResourceDescription sd, ServiceConfiguration sc) {
        ServiceWorker newResource = new ServiceWorker(wsdl, sd, sc);
        return newResource;
    }

    private static CloudProvider addProvider() {
        String providerName = "Provider" + (int) (Math.random() * 10000);
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = ResourceManager.registerCloudProvider(providerName, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
        }

        String imageName = "IMAGE" + (int) (Math.random() * 10000);
        CloudImageDescription cid = new CloudImageDescription(imageName, new HashMap<>());
        cp.addCloudImage(cid);

        String typeName = "TYPE" + (int) (Math.random() * 10000);
        float type1Memory = (float) Math.random() * 5;
        MethodResourceDescription mrd1 = new MethodResourceDescription();
        mrd1.setMemorySize(type1Memory);
        CloudInstanceTypeDescription citd = new CloudInstanceTypeDescription(typeName, mrd1);
        cp.addInstanceType(citd);
        return cp;
    }

    private CloudMethodResourceDescription createResourceDescriptionFromProvider(CloudProvider cp1) {
        CloudImageDescription cid = null;
        CloudInstanceTypeDescription citd = null;
        for (String imageName : cp1.getAllImageNames()) {
            cid = cp1.getImage(imageName);
            break;
        }
        for (String instanceType : cp1.getAllInstanceTypeNames()) {
            citd = cp1.getInstanceType(instanceType);
            break;
        }
        return new CloudMethodResourceDescription(citd, cid);
    }
}
