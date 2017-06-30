package integratedtoolkit.util;

import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.types.ExtendedCloudMethodWorker;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.fake.FakeNode;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudInstanceTypeDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import java.util.HashMap;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author flordan
 */
public class CloudManagerTest {

    public static final String RUNTIME_CONNECTOR = "integratedtoolkit.connectors.fake.FakeConnector";

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testEmpty() {
        CloudManager cm = new CloudManager();
        if (cm.isUseCloud()) {
            fail("Empty cloud Manager should not indicate cloud is enabled");
        }
        if (cm.getCurrentVMCount() != 0) {
            fail("Empty cloud Manager should not contain any cloud instance");
        }
        if (cm.getMinVMs() != 0) {
            fail("Empty cloud Manager should have no bottom boundary on the number of instances");
        }
        if (cm.getMaxVMs() != Integer.MAX_VALUE) {
            fail("Empty cloud Manager should have no top boundary on the number of instances");
        }
        if (cm.getInitialVMs() != 0) {
            fail("Empty cloud Manager should have no initial instance requirement");
        }
    }

    @Test
    public void testBondaries() {
        CloudManager cm = new CloudManager();
        cm.setMaxVMs(5);
        if (cm.getMinVMs() != 0) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 0) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMinVMs(1);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setInitialVMs(2);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMinVMs(3);
        if (cm.getMinVMs() != 3) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 3) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMinVMs(1);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMinVMs(7);
        if (cm.getMinVMs() != 7) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 7) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 7) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMinVMs(1);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setInitialVMs(6);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

        cm.setMaxVMs(7);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 7) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 6) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }
        cm.setInitialVMs(2);
        cm.setMaxVMs(5);

        //Check ignoring nulls
        cm.setMinVMs(null);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }
        cm.setInitialVMs(null);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }
        cm.setMaxVMs(null);
        if (cm.getMinVMs() != 1) {
            fail("Cloud Manager does not properly maintain the Min VMs boundary");
        }
        if (cm.getMaxVMs() != 5) {
            fail("Cloud Manager does not properly maintain the Max VMs boundary");
        }
        if (cm.getInitialVMs() != 2) {
            fail("Cloud Manager does not properly maintain the Initial VMs boundary");
        }

    }

    @Test
    public void testAddProviders() {
        CloudManager cm = new CloudManager();

        CloudProvider cp = createProvider(cm);

        if (!cm.isUseCloud()) {
            fail("Cloud Manager does not notice that Cloud is enabled when a provider is added.");
        }

        if ((cm.getProviders().size()) != 1) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (!cm.getProviders().contains(cp)) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (cm.getProvider(cp.getName()) != cp) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }

        CloudProvider cp2 = createProvider(cm);
        if ((cm.getProviders().size()) != 2) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (!cm.getProviders().contains(cp)) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (!cm.getProviders().contains(cp2)) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (cm.getProvider(cp.getName()) != cp) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
        if (cm.getProvider(cp2.getName()) != cp2) {
            fail("Cloud Manager does not properly register Cloud Providers.");
        }
    }

    @Test
    public void testVMsManagement() {
        CloudManager cm = new CloudManager();

        CloudProvider cp1 = createProvider(cm);
        CloudProvider cp2 = createProvider(cm);

        CloudMethodResourceDescription cmrd1 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr1 = cp1.requestResourceCreation(cmrd1);
        if (rcr1 == null) {
            fail("Cloud Manager could not create the requested resource.");
        }
        if (cm.getCurrentVMCount() != 1) {
            fail("Cloud Manager is not properly counting the number of requested VMs");
        }
        if (cm.getPendingRequests().size() != 1) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr1)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        CloudMethodResourceDescription cmrd2 = createResourceDescriptionFromProvider(cp2);
        ResourceCreationRequest rcr2 = cp2.requestResourceCreation(cmrd2);
        if (rcr2 == null) {
            fail("Cloud Manager could not create the requested resource.");
        }
        if (cm.getCurrentVMCount() != 2) {
            fail("Cloud Manager is not properly counting the number of requested VMs");
        }
        if (cm.getPendingRequests().size() != 2) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr1)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr2)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        CloudMethodResourceDescription cmrd3 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr3 = cp1.requestResourceCreation(cmrd3);
        if (rcr3 == null) {
            fail("Cloud Manager could not create the requested resource.");
        }
        if (cm.getCurrentVMCount() != 3) {
            fail("Cloud Manager is not properly counting the number of requested VMs");
        }
        if (cm.getPendingRequests().size() != 3) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr1)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr2)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }
        if (!cm.getPendingRequests().contains(rcr3)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs");
        }

        cp1.refusedCreation(rcr3);
        if (cm.getCurrentVMCount() != 2) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (cm.getPendingRequests().size() != 2) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (!cm.getPendingRequests().contains(rcr1)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (!cm.getPendingRequests().contains(rcr2)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (cm.getPendingRequests().contains(rcr3)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        String vmName1 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw1 = new ExtendedCloudMethodWorker(vmName1, cp1, cmrd1, new FakeNode(vmName1), 0, new HashMap<>());
        cp1.confirmedCreation(rcr1, cmw1, cmrd1);

        if (cm.getCurrentVMCount() != 2) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (cm.getPendingRequests().size() != 1) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (cm.getPendingRequests().contains(rcr1)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (!cm.getPendingRequests().contains(rcr2)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (!cp1.getHostedWorkers().contains(cmw1)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }

        String vmName2 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw2 = new ExtendedCloudMethodWorker(vmName2, cp2, cmrd2, new FakeNode(vmName2), 0, new HashMap<>());
        cp2.confirmedCreation(rcr2, cmw2, cmrd2);

        if (cm.getCurrentVMCount() != 2) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (!cm.getPendingRequests().isEmpty()) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (cm.getPendingRequests().contains(rcr2)) {
            fail("Cloud Manager is not properly keeping track of the requested VMs when refusing creation requests");
        }
        if (!cp2.getHostedWorkers().contains(cmw2)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }

        CloudMethodResourceDescription reduction2 = new CloudMethodResourceDescription(cmrd2);
        cmw2.getDescription().reduce(reduction2);
        cp2.requestResourceReduction(cmw2, reduction2);
        if (cm.getCurrentVMCount() != 1) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (cp2.getHostedWorkers().contains(cmw2)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        if (!cp1.getHostedWorkers().contains(cmw1)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        if (!cmw2.isTerminated()) {
            fail("Cloud Manager did not called the connector to terminate the resource");
        }
        CloudMethodResourceDescription reduction1 = new CloudMethodResourceDescription(cmrd1);
        cmw1.getDescription().reduce(reduction1);
        cp1.requestResourceReduction(cmw1, reduction1);
        if (cm.getCurrentVMCount() != 0) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (cp1.getHostedWorkers().contains(cmw1)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        if (!cmw1.isTerminated()) {
            fail("Cloud Manager did not called the connector to terminate the resource");
        }
    }

    @Test
    public void testTermianteAll() throws Exception {
        CloudManager cm = new CloudManager();

        CloudProvider cp1 = createProvider(cm);
        CloudProvider cp2 = createProvider(cm);

        CloudMethodResourceDescription cmrd1 = createResourceDescriptionFromProvider(cp1);
        ResourceCreationRequest rcr1 = cp1.requestResourceCreation(cmrd1);
        String vmName1 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw1 = new ExtendedCloudMethodWorker(vmName1, cp1, cmrd1, new FakeNode(vmName1), 0, new HashMap<>());
        cp1.confirmedCreation(rcr1, cmw1, cmrd1);

        CloudMethodResourceDescription cmrd2 = createResourceDescriptionFromProvider(cp2);
        ResourceCreationRequest rcr2 = cp2.requestResourceCreation(cmrd2);
        String vmName2 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw2 = new ExtendedCloudMethodWorker(vmName2, cp2, cmrd2, new FakeNode(vmName2), 0, new HashMap<>());
        cp2.confirmedCreation(rcr2, cmw2, cmrd2);

        if (cm.getCurrentVMCount() != 2) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (!cp2.getHostedWorkers().contains(cmw2)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        if (!cp1.getHostedWorkers().contains(cmw1)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        cm.terminateALL();
        if (cm.getCurrentVMCount() != 0) {
            fail("Cloud Manager is not properly counting the number of requested VMs when refusing creation requests");
        }
        if (cp2.getHostedWorkers().contains(cmw2)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
        if (cp1.getHostedWorkers().contains(cmw1)) {
            fail("Cloud Manager is not properly keeping track of the VMs hosted in a cloud provider");
        }
    }

    private static CloudProvider createProvider(CloudManager cm) {
        String providerName = "Provider" + (int) (Math.random() * 10000);
        HashMap<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = cm.registerCloudProvider(providerName, 0, RUNTIME_CONNECTOR, null, null, properties);
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
