package integratedtoolkit.types.resources.test;

import static org.junit.Assert.*;
import integratedtoolkit.types.resources.ResourcesFile;
import integratedtoolkit.types.resources.exceptions.ResourcesFileValidationException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;


public class ResourcesFileTest {
	
	// Schema
	private static final String SCHEMA_PATH 			= "resources_schema.xsd";
	
	// Default XMLs
	private static final String DEFAULT_XML_PATH 		= "default_resources.xml";
	private static final String FULL_XML_PATH 			= "examples/full/resources.xml";
	private static final String GRID_XML_PATH 			= "examples/grid/resources.xml";
	private static final String SERVICES_XML_PATH 		= "examples/services/resources.xml";
	private static final String ROCCI_XML_PATH 			= "examples/cloud/rocci/resources.xml";
	private static final String JCLOUDS_GCE_XML_PATH 	= "examples/cloud/jclouds/resources_gce.xml";
	
	// TMP XML file
	private static final String XML_TMP_PATH			= "tmp_resources.xml";
	
	// Error Messages
	private static final String ERROR_SCHEMA	 		= "XSD Schema doesn't exist";
	private static final String ERROR_DEFAULT_XML 		= "Default XML doesn't exist";
	private static final String ERROR_FULL_XML 			= "Full XML doesn't exist";
	private static final String ERROR_GRID_XML 			= "Grid XML doesn't exist";
	private static final String ERROR_SERVICES_XML 		= "Services XML doesn't exist";
	private static final String ERROR_ROCCI_XML 		= "Rocci XML doesn't exist";
	private static final String ERROR_JCLOUDS_XML 		= "JClouds XML doesn't exist";
	
	// Test Logger
	private static final Logger logger = Logger.getLogger("test.xml.resources");
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		// Initialize logger
		BasicConfigurator.configure();
		
		// Check existance of all files
		if ( !(new File(SCHEMA_PATH)).exists() ) {
			throw new Exception(ERROR_SCHEMA);
		}
		if ( !(new File(DEFAULT_XML_PATH)).exists() ) {
			throw new Exception(ERROR_DEFAULT_XML);
		}
		if ( !(new File(FULL_XML_PATH)).exists() ) {
			throw new Exception(ERROR_FULL_XML);
		}
		if ( !(new File(GRID_XML_PATH)).exists() ) {
			throw new Exception(ERROR_GRID_XML);
		}
		if ( !(new File(SERVICES_XML_PATH)).exists() ) {
			throw new Exception(ERROR_SERVICES_XML);
		}
		if ( !(new File(ROCCI_XML_PATH)).exists() ) {
			throw new Exception(ERROR_ROCCI_XML);
		}
		if ( !(new File(JCLOUDS_GCE_XML_PATH)).exists() ) {
			throw new Exception(ERROR_JCLOUDS_XML);
		}
		
		// Reset the TMP file if needed
		if ( (new File(XML_TMP_PATH)).exists() ) {
			(new File(XML_TMP_PATH)).delete();
		}
	}
	
	@AfterClass
	public static void afterClass() throws Exception {
		if ( (new File(XML_TMP_PATH)).exists() ) {
			(new File(XML_TMP_PATH)).delete();
		}
	}
	
	
	/* ***************************************************************
	 * CONSTRUCTOR TESTS
	 * ***************************************************************/
	@Test
	public void creation_XMLfile_XSDschema() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
		// Instantiate XSD Schema
		SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI); 
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
		File defaultResources = new File(DEFAULT_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd, logger);
		
		// Checkers
		assertNotNull(resources);
		
		int numWorkers_byName = resources.getComputeNodes_names().size();
		assertEquals("Should have a worker", numWorkers_byName, 1);
		
		int numWorkers_byList = resources.getComputeNodes_list().size();
		assertEquals("Should have a worker", numWorkers_byList, 1);
		
		int numWorkers_byHashMap = resources.getComputeNodes_hashMap().size();
		assertEquals("Should have a worker", numWorkers_byHashMap, 1);
	}
	
	@Test
	public void creation_XMLstring_XSDschema() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
		// Instantiate XSD Schema
		SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI); 
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
		String defaultResources = buildXMLString();
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd, logger);
		
		// Checkers
		assertNotNull(resources);
		
		int numWorkers_byName = resources.getComputeNodes_names().size();
		assertEquals("Should have a worker", numWorkers_byName, 1);
		
		int numWorkers_byList = resources.getComputeNodes_list().size();
		assertEquals("Should have a worker", numWorkers_byList, 1);
		
		int numWorkers_byHashMap = resources.getComputeNodes_hashMap().size();
		assertEquals("Should have a worker", numWorkers_byHashMap, 1);
	}
	
	@Test
	public void creation_XMLfile_XSDpath() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File defaultResources = new File(DEFAULT_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		int numWorkers_byName = resources.getComputeNodes_names().size();
		assertEquals("Should have a worker", numWorkers_byName, 1);
		
		int numWorkers_byList = resources.getComputeNodes_list().size();
		assertEquals("Should have a worker", numWorkers_byList, 1);
		
		int numWorkers_byHashMap = resources.getComputeNodes_hashMap().size();
		assertEquals("Should have a worker", numWorkers_byHashMap, 1);
	}
	
	@Test
	public void creation_XMLstring_XSDpath() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		String defaultResources = buildXMLString();
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		int numWorkers_byName = resources.getComputeNodes_names().size();
		assertEquals("Should have a worker", numWorkers_byName, 1);
		
		int numWorkers_byList = resources.getComputeNodes_list().size();
		assertEquals("Should have a worker", numWorkers_byList, 1);
		
		int numWorkers_byHashMap = resources.getComputeNodes_hashMap().size();
		assertEquals("Should have a worker", numWorkers_byHashMap, 1);
	}
	
	
	/* ***************************************************************
	 * Dumpers checkers
	 * ***************************************************************/
	@Test
	public void XMLtoFile() throws URISyntaxException, SAXException, JAXBException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File defaultResources = new File(DEFAULT_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(defaultResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	@Test 
	public void XMLtoString() throws URISyntaxException, SAXException, JAXBException, ResourcesFileValidationException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File defaultResources = new File(DEFAULT_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(defaultResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		String real_xml = resources.getString();
		String ideal_xml = buildXMLString();
		assertEquals("To string method doesn't produce the correct dump", real_xml, ideal_xml);
	}
	
	
	/* ***************************************************************
	 * Different types of XML checkers
	 * ***************************************************************/
	@Test
	public void fullXML() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File fullResources = new File(FULL_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(fullResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(fullResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	@Test
	public void gridXML() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File gridResources = new File(GRID_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(gridResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(gridResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	@Test
	public void servicesXML() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File servicesResources = new File(SERVICES_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(servicesResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(servicesResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	@Test
	public void rocciXML() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File rocciResources = new File(ROCCI_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(rocciResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(rocciResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	@Test
	public void jclouds_gceXML() throws URISyntaxException, JAXBException, SAXException, ResourcesFileValidationException, IOException {
		// Get XSD Schema path
		String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
		File gceResources = new File(JCLOUDS_GCE_XML_PATH);
		
		// Instantiate ResourcesFile
		ResourcesFile resources =  new ResourcesFile(gceResources, xsd_path, logger);
		
		// Checkers
		assertNotNull(resources);
		
		File xml = new File(XML_TMP_PATH);
		resources.toFile(xml);
		boolean compareFile = FileUtils.contentEquals(gceResources, xml);
		assertEquals("Dump content not equal", true, compareFile);
	}
	
	
	/* ***************************************************************
	 * XML String builder (PRIVATE)
	 * ***************************************************************/
	private String buildXMLString() {
		StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<ResourcesList>").append("\n");
        
        sb.append("    ").append("<ComputeNode Name=\"localhost\">").append("\n");
        
        sb.append("    ").append("    ").append("<Processor Name=\"MainProcessor\">").append("\n");
        sb.append("    ").append("    ").append("    ").append("<ComputingUnits>4</ComputingUnits>").append("\n");
        sb.append("    ").append("    ").append("</Processor>").append("\n");
        
        sb.append("    ").append("    ").append("<Adaptors>").append("\n");
        sb.append("    ").append("    ").append("    ").append("<Adaptor Name=\"integratedtoolkit.nio.master.NIOAdaptor\">").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Interactive/>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<Ports>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<MinPort>43001</MinPort>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<MaxPort>43002</MaxPort>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</Ports>").append("\n");
        sb.append("    ").append("    ").append("    ").append("</Adaptor>").append("\n");
        sb.append("    ").append("    ").append("    ").append("<Adaptor Name=\"integratedtoolkit.gat.master.GATAdaptor\">").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Batch>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("    ").append("<Queue>sequential</Queue>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("</Batch>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("    ").append("<Interactive/>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("</SubmissionSystem>").append("\n");
        sb.append("    ").append("    ").append("    ").append("    ").append("<BrokerAdaptor>sshtrilead</BrokerAdaptor>").append("\n");
        sb.append("    ").append("    ").append("    ").append("</Adaptor>").append("\n");
        sb.append("    ").append("    ").append("</Adaptors>").append("\n");
        sb.append("    ").append("</ComputeNode>").append("\n");

        sb.append("</ResourcesList>").append("\n");
        
        return sb.toString();
	}
	
}
