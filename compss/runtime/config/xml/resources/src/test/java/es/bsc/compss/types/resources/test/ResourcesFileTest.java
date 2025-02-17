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
package es.bsc.compss.types.resources.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import es.bsc.compss.types.resources.ResourcesFile;
import es.bsc.compss.types.resources.exceptions.InvalidElementException;
import es.bsc.compss.types.resources.exceptions.ResourcesFileValidationException;
import es.bsc.compss.types.resources.jaxb.AdaptorType;
import es.bsc.compss.types.resources.jaxb.AdaptorsListType;
import es.bsc.compss.types.resources.jaxb.CloudProviderType;
import es.bsc.compss.types.resources.jaxb.ComputeNodeType;
import es.bsc.compss.types.resources.jaxb.DataNodeType;
import es.bsc.compss.types.resources.jaxb.EndpointType;
import es.bsc.compss.types.resources.jaxb.ImageType;
import es.bsc.compss.types.resources.jaxb.ImagesType;
import es.bsc.compss.types.resources.jaxb.InstanceTypeType;
import es.bsc.compss.types.resources.jaxb.InstanceTypesType;
import es.bsc.compss.types.resources.jaxb.OSTypeType;
import es.bsc.compss.types.resources.jaxb.ProcessorPropertyType;
import es.bsc.compss.types.resources.jaxb.ProcessorType;
import es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ServiceType;
import es.bsc.compss.types.resources.jaxb.SharedDiskType;

import jakarta.xml.bind.JAXBException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;


public class ResourcesFileTest {

    // Schema
    private static final String SCHEMA_PATH = "resources_schema.xsd";

    // Default XMLs
    private static final String DEFAULT_XML_PATH = "default_resources.xml";
    private static final String FULL_XML_PATH = "examples/full/resources.xml";
    private static final String GRID_XML_PATH = "examples/grid/resources.xml";
    private static final String SERVICES_XML_PATH = "examples/services/resources.xml";
    private static final String ROCCI_XML_PATH = "examples/cloud/rocci/resources.xml";
    private static final String JCLOUDS_GCE_XML_PATH = "examples/cloud/jclouds/resources_gce.xml";

    // TMP XML file
    private static final String XML_TMP_PATH = "tmp_resources.xml";

    // Error Messages
    private static final String ERROR_SCHEMA = "XSD Schema doesn't exist";
    private static final String ERROR_DEFAULT_XML = "Default XML doesn't exist";
    private static final String ERROR_FULL_XML = "Full XML doesn't exist";
    private static final String ERROR_GRID_XML = "Grid XML doesn't exist";
    private static final String ERROR_SERVICES_XML = "Services XML doesn't exist";
    private static final String ERROR_ROCCI_XML = "Rocci XML doesn't exist";
    private static final String ERROR_JCLOUDS_XML = "JClouds XML doesn't exist";

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");


    /**
     * Before class method.
     * 
     * @throws Exception Error in before class
     */
    @BeforeClass
    public static void beforeClass() throws Exception {
        // Initialize logger
        // No longer needed since log4j2 automatically sets it up on error level
        // BasicConfigurator.configure();

        // Check existance of all files
        if (!(new File(SCHEMA_PATH)).exists()) {
            throw new Exception(ERROR_SCHEMA);
        }
        if (!(new File(DEFAULT_XML_PATH)).exists()) {
            throw new Exception(ERROR_DEFAULT_XML);
        }
        if (!(new File(FULL_XML_PATH)).exists()) {
            throw new Exception(ERROR_FULL_XML);
        }
        if (!(new File(GRID_XML_PATH)).exists()) {
            throw new Exception(ERROR_GRID_XML);
        }
        if (!(new File(SERVICES_XML_PATH)).exists()) {
            throw new Exception(ERROR_SERVICES_XML);
        }
        if (!(new File(ROCCI_XML_PATH)).exists()) {
            throw new Exception(ERROR_ROCCI_XML);
        }
        if (!(new File(JCLOUDS_GCE_XML_PATH)).exists()) {
            throw new Exception(ERROR_JCLOUDS_XML);
        }

        // Reset the TMP file if needed
        if ((new File(XML_TMP_PATH)).exists()) {
            (new File(XML_TMP_PATH)).delete();
        }
    }

    /**
     * After class method.
     * 
     * @throws Exception Error in after class method
     */
    @AfterClass
    public static void afterClass() throws Exception {
        if ((new File(XML_TMP_PATH)).exists()) {
            (new File(XML_TMP_PATH)).delete();
        }
    }

    /* ****************** CONSTRUCTOR TESTS *******************/
    @Test
    public void creation_empty() throws JAXBException, InvalidElementException {
        ResourcesFile resources = new ResourcesFile(LOGGER);
        // Checkers
        assertNotNull(resources);
        assertNotNull(resources.addSharedDisk("disk1"));
        assertNotNull(resources.addDataNode("dn_name", "dn_host", "dn_path", new ArrayList<AdaptorType>()));
        assertNotNull(resources.addDataNode("dn_name_2", "dn_host_2", "dn_path", new AdaptorsListType()));
        assertNotNull(resources.addService("http://wsdl", "serviceName", "namespace", "1232"));
        ProcessorType proc = ResourcesFile.createProcessor("cpu_1", 2);
        List<ProcessorType> processors = new ArrayList<>();
        processors.add(proc);
        ComputeNodeType cn =
            resources.addComputeNode("localhost", processors, new ArrayList<AdaptorType>(), 200f, 200f, "Linux");
        assertNotNull(cn);
        CloudProviderType cp =
            resources.addCloudProvider("cp1", new EndpointType(), new ImagesType(), new InstanceTypesType());
        assertNotNull(cp);
        assertNotNull(resources.addCloudProvider("cp3", new EndpointType(), new ArrayList<ImageType>(),
            new ArrayList<InstanceTypeType>()));
        assertNotNull(resources.addCloudProvider("cp2", "server", "jar", "class", new ArrayList<ImageType>(),
            new ArrayList<InstanceTypeType>()));
        ImageType im = ResourcesFile.createImage("imageName", "adaptorName", 45, 23);
        InstanceTypeType it = ResourcesFile.createInstance("Iname", "cpu", 2, "x86", 2000.0f, "CPU", 2000.0f, null,
            2000.0f, "RAM", 2000.0f, "ssd", 200);
        assertTrue(resources.addImageToCloudProvider("cp1", im));
        assertTrue(resources.addInstanceToCloudProvider("cp1", it));
        resources.attachSharedDiskToComputeNode("disk1", "localhost", "/mnt/");
        resources.detachSharedDiskToComputeNode("disk1", "localhost");
        assertTrue(resources.deleteCloudProvider("cp2"));
        assertTrue(resources.deleteComputeNode("localhost"));
        assertTrue(resources.deleteSharedDisk("disk1"));
        assertTrue(resources.deleteDataNode("dn_name"));
        assertTrue(resources.deleteService("http://wsdl"));

    }

    @Test
    public void creation_XMLfile_XSDschema()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        File defaultResources = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsd, LOGGER);

        // Checkers
        assertNotNull(resources);

        int numWorkersByName = resources.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = resources.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = resources.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDschema()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        String defaultResources = buildXMLString();

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsd, LOGGER);

        // Checkers
        assertNotNull(resources);

        int numWorkersByName = resources.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = resources.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = resources.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLfile_XSDpath()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultResources = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        int numWorkersByName = resources.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = resources.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = resources.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDpath()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        String defaultResources = buildXMLString();

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        int numWorkersByName = resources.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = resources.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = resources.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    /*
     * *************************************************************** Dumpers checkers
     ***************************************************************/
    @Test
    public void xmlToFile()
        throws URISyntaxException, SAXException, JAXBException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultResources = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(defaultResources, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void xmltoString() throws URISyntaxException, SAXException, JAXBException, ResourcesFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultResources = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(defaultResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        String realXml = resources.getString();
        String idealXml = buildXMLString();
        assertEquals("To string method doesn't produce the correct dump", realXml, idealXml);
    }

    /*
     * *************************************************************** Different types of XML checkers
     ***************************************************************/
    @Test
    public void fullXML()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File fullResources = new File(FULL_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(fullResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(fullResources, xml);
        assertEquals("Dump content not equal", true, compareFile);

        // Check service
        assertNotNull(resources.getService("http://localhost:8080/my-service?wsdl"));

        // Checking data nodes
        List<DataNodeType> dnLst = resources.getDataNodes_list();
        assertEquals(dnLst.size(), 4);
        HashMap<String, DataNodeType> dnHM = resources.getDataNodes_hashMap();
        assertEquals(dnHM.size(), 4);
        List<String> dnNames = resources.getDataNodes_names();
        assertEquals(dnNames.size(), 4);
        DataNodeType dn = resources.getDataNode("DNFull");
        assertNotNull(dn);
        assertEquals(resources.getHost(dn), "localhost");
        assertEquals(resources.getPath(dn), "/tmp/");
        assertEquals(resources.getStorageSize(dn), 2000.0f, 0.001);
        assertEquals(resources.getStorageType(dn), "persistent");
        assertEquals(resources.getStorageBW(dn), 20);

        // Checking Shared disks
        HashMap<String, SharedDiskType> sdHM = resources.getSharedDisks_hashMap();
        assertEquals(sdHM.size(), 3);
        List<SharedDiskType> sdLst = resources.getSharedDisks_list();
        assertEquals(sdLst.size(), 3);
        List<String> sdNames = resources.getSharedDisks_names();
        assertEquals(sdNames.size(), 3);
        SharedDiskType sd = resources.getSharedDisk("DiskFull");
        assertNotNull(sd);
        ComputeNodeType cn = resources.getComputeNode("localhost_full");
        assertNotNull(sd);
        HashMap<String, String> sdCN = resources.getSharedDisks(cn);
        assertEquals(sdCN.size(), 1);
        HashMap<String, String> sdMPs = resources.getDiskMountPointsInComputeNodes("DiskFull");
        assertEquals(sdMPs.size(), 1);
        assertEquals(resources.getMemoryType(cn), "Non-volatile");
        assertEquals(resources.getStorageSize(cn), 2000.0f, 0.001);
        assertNull(resources.getStorageType(cn));
        assertEquals(resources.getStorageBW(cn), -1);
        assertEquals(resources.getOperatingSystemType(cn), "Linux");
        assertEquals(resources.getOperatingSystemDistribution(cn), "OpenSUSE");
        assertEquals(resources.getOperatingSystemVersion(cn), "13.2");
        assertEquals(resources.getApplications(cn).size(), 2);
        assertEquals(resources.getPrice(cn).getPricePerUnit(), 0.12f, 0.001f);

        // CloudProviders
        List<CloudProviderType> cpLst = resources.getCloudProviders_list();
        assertEquals(cpLst.size(), 2);
        HashMap<String, CloudProviderType> cpHM = resources.getCloudProviders_hashMap();
        assertEquals(cpHM.size(), 2);
        List<String> cpNames = resources.getCloudProviders_names();
        assertEquals(cpNames.size(), 2);
        CloudProviderType cp = resources.getCloudProvider("BSC_full");
        assertNotNull(cp);
        assertEquals(resources.getServer(cp.getEndpoint()), "https://bscgrid20.bsc.es");
        assertEquals(resources.getConnectorJarPath(cp.getEndpoint()), "rocci-conn.jar");
        assertEquals(resources.getConnectorMainClass(cp.getEndpoint()), "es.bsc.conn.rocci.ROCCI");
        assertEquals(resources.getPort(cp.getEndpoint()), "11443");
        InstanceTypeType it = resources.getInstance(cp, "Instance_full");
        assertNotNull(it);
        assertEquals(resources.getPrice(it).getPricePerUnit(), 36.0f, 0.01f);
        assertEquals(resources.getStorageSize(it), 2000.0f, 0.001);
        assertEquals(resources.getStorageType(it), "persistent");
        assertEquals(resources.getStorageBW(it), -1);
        assertEquals(resources.getProcessors(it).size(), 2);
        assertEquals(resources.getMemorySize(it), 1000.0f, 0.001);
        assertNull(resources.getMemoryType(it));
        ImageType im = resources.getImage(cp, "Image_bsc_full");
        assertNotNull(im);
        assertEquals(resources.getOperatingSystemType(im), "Linux");
        assertNull(resources.getOperatingSystemDistribution(im));
        assertNull(resources.getOperatingSystemVersion(im));
        assertEquals(resources.getApplications(im).get(0), "Java");
        assertEquals(resources.getPrice(im).getPricePerUnit(), 36.0f, 0.01f);
        HashMap<String, String> sdIM = resources.getSharedDisks(im);
        assertEquals(sdIM.size(), 1);
        assertEquals(resources.getAdaptorProperties(im, "es.bsc.compss.nio.master.NIOAdaptor").size(), 1);
        assertEquals(resources.getAdaptorQueues(im, "es.bsc.compss.nio.master.NIOAdaptor").size(), 0);

    }

    @Test
    public void gridXML()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gridResources = new File(GRID_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(gridResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gridResources, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void servicesXML()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File servicesResources = new File(SERVICES_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(servicesResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(servicesResources, xml);
        assertEquals("Dump content not equal", true, compareFile);
        List<ServiceType> servs = resources.getServices_list();
        assertEquals(servs.size(), 1);
        HashMap<String, ServiceType> servHM = resources.getServices_hashMap();
        assertEquals(servHM.size(), 1);
        List<String> servNames = resources.getServices_names();
        assertEquals(servNames.size(), 1);
        assertEquals(servNames.get(0), "HmmerObjects");
        HashMap<String, List<ServiceType>> servByNames = resources.getServices_byName();
        assertEquals(servByNames.size(), 1);
    }

    @Test
    public void rocciXML()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File rocciResources = new File(ROCCI_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(rocciResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(rocciResources, xml);
        assertEquals("Dump content not equal", true, compareFile);
        CloudProviderType cp = resources.getCloudProvider("ProviderName");
        assertNotNull(cp);
        ImageType im = resources.getImage(cp, "EGI_compss");
        assertNotNull(im);
        assertEquals(resources.getCreationTime(im), 60);

    }

    @Test
    public void jclouds_gceXML()
        throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gceResources = new File(JCLOUDS_GCE_XML_PATH);

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(gceResources, xsdPath, LOGGER);

        // Checkers
        assertNotNull(resources);

        File xml = new File(XML_TMP_PATH);
        resources.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gceResources, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    /*
     * *************************************************************** XML String builder (PRIVATE)
     ***************************************************************/
    private String buildXMLString() {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<ResourcesList>").append("\n");

        sb.append("    ").append("<ComputeNode Name=\"localhost\">").append("\n");

        sb.append("    ").append("    ").append("<Processor Name=\"MainProcessor\">").append("\n");
        sb.append("    ").append("    ").append("    ").append("<ComputingUnits>4</ComputingUnits>").append("\n");
        sb.append("    ").append("    ").append("</Processor>").append("\n");

        sb.append("    ").append("    ").append("<Adaptors>").append("\n");
        sb.append("    ").append("    ").append("    ").append("<Adaptor Name=\"es.bsc.compss.nio.master.NIOAdaptor\">")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Interactive/>")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<Ports>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<MinPort>43001</MinPort>")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<MaxPort>43002</MaxPort>")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</Ports>").append("\n");
        sb.append("    ").append("    ").append("    ").append("</Adaptor>").append("\n");
        sb.append("    ").append("    ").append("    ").append("<Adaptor Name=\"es.bsc.compss.gat.master.GATAdaptor\">")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Batch>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("    ")
            .append("<Queue>sequential</Queue>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("</Batch>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Interactive/>")
            .append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ")
            .append("<BrokerAdaptor>sshtrilead</BrokerAdaptor>").append("\n");
        sb.append("    ").append("    ").append("    ").append("</Adaptor>").append("\n");
        sb.append("    ").append("    ").append("</Adaptors>").append("\n");
        sb.append("    ").append("</ComputeNode>").append("\n");

        sb.append("</ResourcesList>").append("\n");

        return sb.toString();
    }

    @Test
    public void staticProcessorAdaptorComputeNodeCreationAndRead()
        throws SAXException, JAXBException, ResourcesFileValidationException {
        int cu = 5;
        float speed = 2.6f;
        String nodeName = "blablahost";
        String procName = "Proc1";
        String arch = "amd64";
        String key = "procKey";
        String value = "procValue";
        String adaptorName = "nio";
        boolean batch = false;
        boolean interactive = true;
        String gatprop = "gat_prop";
        String user = "user";

        ProcessorPropertyType pp = ResourcesFile.createProcessorProperty(key, value);
        ProcessorType proc = ResourcesFile.createProcessor(procName, cu, arch, speed, "CPU", 0.0f, pp);
        AdaptorType ad = ResourcesFile.createAdaptor(adaptorName, batch, null, interactive, gatprop, user);
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(xsdPath, LOGGER);
        List<ProcessorType> processors = new LinkedList<ProcessorType>();
        processors.add(proc);
        List<AdaptorType> adaptors = new LinkedList<AdaptorType>();
        adaptors.add(ad);
        resources.addComputeNode(nodeName, processors, adaptors);
        ComputeNodeType cn = resources.getComputeNode(nodeName);
        ProcessorType procExtracted = resources.getProcessors(cn).get(0);
        assertEquals(procName, procExtracted.getName());
        assertEquals(cu, resources.getProcessorComputingUnits(procExtracted));
        assertEquals(arch, resources.getProcessorArchitecture(procExtracted));
        assertEquals(speed, resources.getProcessorSpeed(procExtracted), 0);
        assertEquals("CPU", resources.getProcessorType(procExtracted));
        assertEquals(-1.0f, resources.getProcessorMemorySize(procExtracted), 0);
        assertEquals(key, resources.getProcessorProperty(procExtracted).getKey());
        assertEquals(value, resources.getProcessorProperty(procExtracted).getValue());
    }

    @Test
    public void computeNodeCreationWithNIOAndRead()
        throws SAXException, JAXBException, ResourcesFileValidationException {
        int cu = 5;
        float speed = 2.6f;
        String nodeName = "blablahost";
        String procName = "Proc1";
        String arch = "amd64";
        String type = "GPU";
        float internalMemorySize = 0.01f;
        String key = "procKey";
        String value = "procValue";
        float memSize = 32.5f;
        float storageSize = 256.0f;
        String osType = OSTypeType.LINUX.value();
        String adaptorName = "nio";
        int minPort = 20;
        int maxPort = 40;
        String executor = "ssh";
        // boolean batch = true;
        // String queue = "default";
        // boolean interactive = true;
        // String gatprop = "sshtrillead";
        String user = "user";

        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(xsdPath, LOGGER);
        resources.addComputeNode(nodeName, procName, cu, arch, speed, type, internalMemorySize,
            ResourcesFile.createProcessorProperty(key, value), adaptorName, maxPort, minPort, executor, user, memSize,
            null, storageSize, null, -1, osType, null, null);
        ComputeNodeType cn = resources.getComputeNode(nodeName);
        ProcessorType procExtracted = resources.getProcessors(cn).get(0);
        assertEquals(procName, procExtracted.getName());
        assertEquals(cu, resources.getProcessorComputingUnits(procExtracted));
        assertEquals(arch, resources.getProcessorArchitecture(procExtracted));
        assertEquals(speed, resources.getProcessorSpeed(procExtracted), 0);
        assertEquals(type, resources.getProcessorType(procExtracted));
        assertEquals(internalMemorySize, resources.getProcessorMemorySize(procExtracted), 0);
        assertEquals(key, resources.getProcessorProperty(procExtracted).getKey());
        assertEquals(value, resources.getProcessorProperty(procExtracted).getValue());
        assertEquals(memSize, resources.getMemorySize(cn), 0);
        assertEquals(storageSize, resources.getStorageSize(cn), 0);

        assertEquals(osType, resources.getOperatingSystemType(cn));

        assertEquals(ResourcesNIOAdaptorProperties.class,
            resources.getAdaptorProperties(cn, adaptorName).get("Ports").getClass());
        assertEquals(minPort,
            ((ResourcesNIOAdaptorProperties) resources.getAdaptorProperties(cn, adaptorName).get("Ports"))
                .getMinPort());
        assertEquals(maxPort,
            ((ResourcesNIOAdaptorProperties) resources.getAdaptorProperties(cn, adaptorName).get("Ports"))
                .getMaxPort());
        assertEquals(executor,
            ((ResourcesNIOAdaptorProperties) resources.getAdaptorProperties(cn, adaptorName).get("Ports"))
                .getRemoteExecutionCommand());
    }

    @Test
    public void computeNodeCreationWithGATAndRead()
        throws SAXException, JAXBException, ResourcesFileValidationException {
        int cu = 5;
        float speed = 2.6f;
        String nodeName = "blablahost";
        String procName = "Proc1";
        String arch = "amd64";
        String key = "procKey";
        String value = "procValue";
        float memSize = 32.5f;
        float storageSize = 256.0f;
        String osType = OSTypeType.LINUX.value();
        String adaptorName = "gat";
        // int minPort = 20;
        // int maxPort = 40;
        // String executor ="ssh";
        boolean batch = true;
        String queue = "default";
        List<String> queues = new ArrayList<String>();
        queues.add(queue);
        boolean interactive = true;
        String gatprop = "sshtrillead";
        String user = "user";

        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Instantiate ResourcesFile
        ResourcesFile resources = new ResourcesFile(xsdPath, LOGGER);
        resources.addComputeNode(nodeName, procName, cu, arch, speed, null, -1f,
            ResourcesFile.createProcessorProperty(key, value), adaptorName, batch, queues, interactive, gatprop, user,
            memSize, null, storageSize, null, -1, osType, null, null);
        ComputeNodeType cn = resources.getComputeNode(nodeName);
        ProcessorType procExtracted = resources.getProcessors(cn).get(0);
        assertEquals(procName, procExtracted.getName());
        assertEquals(cu, resources.getProcessorComputingUnits(procExtracted));
        assertEquals(arch, resources.getProcessorArchitecture(procExtracted));
        assertEquals(speed, resources.getProcessorSpeed(procExtracted), 0);
        assertEquals("CPU", resources.getProcessorType(procExtracted));
        assertEquals(key, resources.getProcessorProperty(procExtracted).getKey());
        assertEquals(value, resources.getProcessorProperty(procExtracted).getValue());
        assertEquals(memSize, resources.getMemorySize(cn), 0);
        assertEquals(storageSize, resources.getStorageSize(cn), 0);
        assertEquals(osType, resources.getOperatingSystemType(cn));
        assertEquals(HashMap.class, resources.getAdaptorProperties(cn, adaptorName).getClass());
        assertEquals(gatprop, resources.getAdaptorProperties(cn, adaptorName).get("BrokerAdaptor"));
        assertEquals(queue, resources.getAdaptorQueues(cn, adaptorName).get(0));
    }

}
