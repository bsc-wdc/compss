package integratedtoolkit.types;

import static org.junit.Assert.fail;

import integratedtoolkit.connectors.fake.FakeConnector;
import integratedtoolkit.types.fake.FakeNode;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.description.CloudImageDescription;
import integratedtoolkit.types.resources.description.CloudInstanceTypeDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class CloudProviderTest {

    public static final String PROVIDER_NAME = "Provider" + (int) (Math.random() * 10000);
    public static final String RUNTIME_CONNECTOR = FakeConnector.class.getName();
    public static final String CID_PROPERTY_TEST_TAG = "TAG" + (int) (Math.random() * 10000);


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
    public void testNewProvider() throws Exception {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        if (cp.getName().compareTo(PROVIDER_NAME) != 0) {
            fail("Cloud Provider is not storing the name properly");
        }
    }

    @Test
    public void testOneImage() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        String image1Name = "IMAGE" + (int) (Math.random() * 10000);
        Map<String, String> imageProperties = new HashMap<>();
        String img1PropValue = "VALUE" + (int) (Math.random() * 10000);
        imageProperties.put(CID_PROPERTY_TEST_TAG, img1PropValue);
        CloudImageDescription cid1 = new CloudImageDescription(image1Name, imageProperties);
        cp.addCloudImage(cid1);

        Set<String> imageNames = cp.getAllImageNames();
        if (!imageNames.contains(image1Name)) {
            fail("Cloud Provider is not storing properly the Images. Cannot find the Image name on the one single image scenario.");
        }
        if (imageNames.size() != 1) {
            fail("Cloud Provider is not storing properly the Images. only two images are supposed to be in the group.");
        }
        CloudImageDescription retrieved1 = cp.getImage(image1Name);
        try {
            checkRetrievedImage(retrieved1, image1Name, img1PropValue);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Images. The provider " + e.getMessage()
                    + " on the one single image scenario.");
        }

    }

    @Test
    public void testTwoImages() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        String image1Name = "IMAGE" + (int) (Math.random() * 10000);
        Map<String, String> imageProperties = new HashMap<>();
        String img1PropValue = "VALUE" + (int) (Math.random() * 10000);
        imageProperties.put(CID_PROPERTY_TEST_TAG, img1PropValue);
        CloudImageDescription cid1 = new CloudImageDescription(image1Name, imageProperties);
        cp.addCloudImage(cid1);

        String image2Name = "IMAGE" + (int) (Math.random() * 10000);
        imageProperties = new HashMap<>();
        String img2PropValue = "VALUE" + (int) (Math.random() * 10000);
        imageProperties.put(CID_PROPERTY_TEST_TAG, img2PropValue);
        CloudImageDescription cid2 = new CloudImageDescription(image2Name, imageProperties);
        cp.addCloudImage(cid2);

        Set<String> imageNames = cp.getAllImageNames();
        int contains = 0;
        if (imageNames.contains(image1Name)) {
            contains++;
        }
        if (imageNames.contains(image2Name)) {
            contains++;
        }
        switch (contains) {
            case 0:
                fail("Cloud Provider is not storing properly the Images. Cannot find any image name on the two images scenario.");
                break;
            case 1:
                fail("Cloud Provider is not storing properly the Images. Cannot find one image name on the two images scenario.");
                break;
            default:
                // Works properly
        }

        if (imageNames.size() != 2) {
            fail("Cloud Provider is not storing properly the Images. only two images are supposed to be in the group.");
        }
        CloudImageDescription retrieved1 = cp.getImage(image1Name);
        try {
            checkRetrievedImage(retrieved1, image1Name, img1PropValue);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Images. The provider " + e.getMessage() + " on the two images scenario.");
        }
        CloudImageDescription retrieved2 = cp.getImage(image2Name);
        try {
            checkRetrievedImage(retrieved2, image2Name, img2PropValue);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Images. " + e.getMessage() + " on the two images scenario.");
        }
    }

    public void checkRetrievedImage(CloudImageDescription retrieved, String name, String imgProperty) throws Exception {
        if (retrieved == null) {
            throw new Exception("The provider cannot find the Image by its name");
        }
        if (name.compareTo(retrieved.getImageName()) != 0) {
            throw new Exception("The retrieved image has not the same name");
        }
        Map<String, String> props = retrieved.getProperties();
        if (props == null) {
            throw new Exception("The retrieved image has no properties");
        }
        String value = props.get(CID_PROPERTY_TEST_TAG);
        if (imgProperty.compareTo(value) != 0) {
            throw new Exception("The retrieved image properties have unexpected values");
        }
    }

    @Test
    public void testOneInstanceType() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        String type1Name = "TYPE" + (int) (Math.random() * 10000);
        float type1Memory = (float) Math.random() * 5;
        MethodResourceDescription mrd1 = new MethodResourceDescription();
        mrd1.setMemorySize(type1Memory);
        CloudInstanceTypeDescription citd = new CloudInstanceTypeDescription(type1Name, mrd1);
        cp.addInstanceType(citd);

        Set<String> instanceNames = cp.getAllInstanceTypeNames();
        if (!instanceNames.contains(type1Name)) {
            fail("Cloud Provider is not storing properly the Images. Cannot find the template name with one single template.");
        }
        if (instanceNames.size() != 1) {
            fail("Cloud Provider is not storing properly the Images. only one template is supposed to be in the group.");
        }
        CloudInstanceTypeDescription retrieved1 = cp.getInstanceType(type1Name);
        try {
            checkRetrievedType(retrieved1, type1Name, type1Memory);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Images. The provider " + e.getMessage()
                    + " on the one single template scenario.");
        }
    }

    @Test
    public void testTwoInstanceType() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        String type1Name = "TYPE" + (int) (Math.random() * 10000);
        float type1Memory = (float) Math.random() * 5;
        MethodResourceDescription mrd1 = new MethodResourceDescription();
        mrd1.setMemorySize(type1Memory);
        CloudInstanceTypeDescription citd1 = new CloudInstanceTypeDescription(type1Name, mrd1);
        cp.addInstanceType(citd1);

        String type2Name = "TYPE" + (int) (Math.random() * 10000);
        float type2Memory = (float) Math.random() * 5;
        MethodResourceDescription mrd2 = new MethodResourceDescription();
        mrd2.setMemorySize(type1Memory);
        CloudInstanceTypeDescription citd2 = new CloudInstanceTypeDescription(type2Name, mrd2);
        cp.addInstanceType(citd2);

        Set<String> instanceNames = cp.getAllInstanceTypeNames();
        int contains = 0;
        if (instanceNames.contains(type1Name)) {
            contains++;
        }
        if (instanceNames.contains(type2Name)) {
            contains++;
        }
        switch (contains) {
            case 0:
                fail("Cloud Provider is not storing properly the Templates. Cannot find any template name on the two templates scenario.");
                break;
            case 1:
                fail("Cloud Provider is not storing properly the Templates. Cannot find one template name on the two templates scenario.");
                break;
            default:
                // Works properly
        }

        if (instanceNames.size() != 2) {
            fail("Cloud Provider is not storing properly the Templates. only two templates are supposed to be in the set.");
        }

        CloudInstanceTypeDescription retrieved1 = cp.getInstanceType(type1Name);
        try {
            checkRetrievedType(retrieved1, type1Name, type1Memory);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Templates. The provider " + e.getMessage()
                    + " on the two templates scenario.");
        }
        CloudInstanceTypeDescription retrieved2 = cp.getInstanceType(type2Name);
        try {
            checkRetrievedType(retrieved2, type2Name, type2Memory);
        } catch (Exception e) {
            fail("Cloud Provider is not storing properly the Templates. The provider " + e.getMessage()
                    + " on the two templates scenario.");
        }
    }

    public void checkRetrievedType(CloudInstanceTypeDescription retrieved, String name, float memory) throws Exception {
        if (retrieved == null) {
            throw new Exception("The provider cannot find the type by its name");
        }
    }

    @Test
    public void testTurnOn() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly intialized the number of requested VMs should be 0");
        }

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);
        if (!FakeConnector.getProcessedRequests().contains(crc)) {
            fail("Turn on has not reached the connector");
        }

        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        if (!pendingRequests.contains(crc)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }

        cmrd = new CloudMethodResourceDescription(citd, cid);
        cmrd.addInstance(citd);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd);
        pendingRequests = cp.getPendingRequests();
        if (!pendingRequests.contains(crc)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (!pendingRequests.contains(crc2)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (pendingRequests.size() != 2) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }

        if (!FakeConnector.getProcessedRequests().contains(crc)) {
            fail("Turn on has not reached the connector");
        }
        if (cp.getCurrentVMCount() != 3) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
    }

    @Test
    public void testRefusedOneTurnOn() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly intialized the number of requested VMs should be 0");
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);
        if (!FakeConnector.getProcessedRequests().contains(crc)) {
            fail("Turn on has not reached the connector");
        }
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }

        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        if (!pendingRequests.contains(crc)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }

        cp.refusedCreation(crc);
        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        pendingRequests = cp.getPendingRequests();

        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
    }

    @Test
    public void testRefusedTwoTurnOn() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
        }

        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly intialized the number of requested VMs should be 0");
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);
        if (!FakeConnector.getProcessedRequests().contains(crc)) {
            fail("Turn on has not reached the connector");
        }

        CloudMethodResourceDescription cmrd2 = new CloudMethodResourceDescription(citd, cid);
        cmrd2.addInstance(citd);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd2);
        if (!FakeConnector.getProcessedRequests().contains(crc)) {
            fail("Turn on has not reached the connector");
        }
        if (cp.getCurrentVMCount() != 3) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }

        cp.refusedCreation(crc2);
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        if (!pendingRequests.contains(crc)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        cp.refusedCreation(crc);
        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
    }

    @Test
    public void testCreateOneVMOneResourceSameDescription() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (!pendingRequests.contains(crc)) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (!workers.isEmpty()) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

        String vmName = "VM" + (int) (Math.random() * 1000);
        CloudMethodWorker cmw = new CloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        pendingRequests = cp.getPendingRequests();
        workers = cp.getHostedWorkers();
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

    @Test
    public void testCreateOneVMOneResourceDifferentDescription() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        String vmName = "VM" + (int) (Math.random() * 1000);
        CloudMethodWorker cmw = new CloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        granted.addInstance(citd);
        cp.confirmedCreation(crc, cmw, granted);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

    @Test
    public void testCreateTwoVMTwoResources() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        CloudMethodResourceDescription cmrd2 = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd2);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }

        String vmName = "VM" + (int) (Math.random() * 1000);
        CloudMethodWorker cmw = new CloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());

        String vmName2 = "VM" + (int) (Math.random() * 1000);
        CloudMethodWorker cmw2 = new CloudMethodWorker(vmName2, cp, cmrd, new FakeNode(vmName2), 0, new HashMap<>());

        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

        granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc2, cmw2, granted);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        pendingRequests = cp.getPendingRequests();
        workers = cp.getHostedWorkers();
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 2) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw2)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

    @Test
    public void testCreateTwoVMOneResource() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        CloudMethodResourceDescription cmrd2 = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd2);

        String vmName = "VM" + (int) (Math.random() * 1000);
        CloudMethodWorker cmw = new CloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());

        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (pendingRequests.size() != 1) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

        granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc2, cmw, granted);
        if (cp.getCurrentVMCount() != 2) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        pendingRequests = cp.getPendingRequests();
        workers = cp.getHostedWorkers();
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

    @Test
    public void testDestroyOneVMOneResource() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        String vmName = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw = new ExtendedCloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());
        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);

        CloudMethodResourceDescription reduction = new CloudMethodResourceDescription(citd, cid);
        cmw.getDescription().reduce(reduction);
        cp.requestResourceReduction(cmw, reduction);
        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        List<ResourceCreationRequest> pendingRequests = cp.getPendingRequests();
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (!pendingRequests.isEmpty()) {
            fail("Cloud Provider is not properly registering the pending creations requests");
        }
        if (!workers.isEmpty()) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

    }

    @Test
    public void testDestroyTwoVMTwoResources() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        CloudMethodResourceDescription cmrd2 = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd2);

        String vmName = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw = new ExtendedCloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());

        String vmName2 = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw2 = new ExtendedCloudMethodWorker(vmName, cp, cmrd2, new FakeNode(vmName2), 0, new HashMap<>());

        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);

        granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc2, cmw2, granted);

        CloudMethodResourceDescription reduction = new CloudMethodResourceDescription(citd, cid);
        cmw.getDescription().reduce(reduction);
        cp.requestResourceReduction(cmw, reduction);
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw2)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

        reduction = new CloudMethodResourceDescription(citd, cid);
        cmw2.getDescription().reduce(reduction);
        cp.requestResourceReduction(cmw2, reduction);
        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        workers = cp.getHostedWorkers();
        if (!workers.isEmpty()) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

    @Test
    public void testDestroyTwoVMOneResource() {
        Map<String, String> properties = new HashMap<>();
        CloudProvider cp = null;
        try {
            cp = new CloudProvider(PROVIDER_NAME, 0, RUNTIME_CONNECTOR, null, null, properties);
        } catch (Exception e) {
            fail("Could not create the Cloud Provider");
            return;
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

        CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc = cp.requestResourceCreation(cmrd);

        CloudMethodResourceDescription cmrd2 = new CloudMethodResourceDescription(citd, cid);
        ResourceCreationRequest crc2 = cp.requestResourceCreation(cmrd2);

        String vmName = "VM" + (int) (Math.random() * 1000);
        ExtendedCloudMethodWorker cmw = new ExtendedCloudMethodWorker(vmName, cp, cmrd, new FakeNode(vmName), 0, new HashMap<>());

        CloudMethodResourceDescription granted = new CloudMethodResourceDescription(citd, cid);
        cp.confirmedCreation(crc, cmw, granted);

        granted = new CloudMethodResourceDescription(citd, cid);
        cmw.getDescription().increase(granted);
        cp.confirmedCreation(crc2, cmw, granted);

        CloudMethodResourceDescription reduction = new CloudMethodResourceDescription(citd, cid);
        cmw.getDescription().reduce(reduction);
        cp.requestResourceReduction(cmw, reduction);
        if (cp.getCurrentVMCount() != 1) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        Set<CloudMethodWorker> workers = cp.getHostedWorkers();
        if (workers.size() != 1) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
        if (!workers.contains(cmw)) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }

        reduction = new CloudMethodResourceDescription(citd, cid);
        cmw.getDescription().reduce(reduction);
        cp.requestResourceReduction(cmw, reduction);
        if (cp.getCurrentVMCount() != 0) {
            fail("Cloud Provider is not properly accounting the number of requested VMs");
        }
        workers = cp.getHostedWorkers();
        if (!workers.isEmpty()) {
            fail("Cloud Provider is not properly registering the hosted workers");
        }
    }

}
