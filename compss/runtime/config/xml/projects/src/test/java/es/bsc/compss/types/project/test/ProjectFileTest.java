package es.bsc.compss.types.project.test;

import static org.junit.Assert.*;
import es.bsc.compss.types.project.ProjectFile;
import es.bsc.compss.types.project.exceptions.ProjectFileValidationException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBException;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.commons.io.FileUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.xml.sax.SAXException;


public class ProjectFileTest {

    // Schema
    private static final String SCHEMA_PATH = "project_schema.xsd";

    // Default XMLs
    private static final String DEFAULT_XML_PATH = "default_project.xml";
    private static final String FULL_XML_PATH = "examples/full/project.xml";
    private static final String GRID_XML_PATH = "examples/grid/project.xml";
    private static final String SERVICES_XML_PATH = "examples/services/project.xml";
    private static final String ROCCI_XML_PATH = "examples/cloud/rocci/project.xml";
    private static final String JCLOUDS_GCE_XML_PATH = "examples/cloud/jclouds/project_gce.xml";

    // TMP XML file
    private static final String XML_TMP_PATH = "tmp_project.xml";

    // Error Messages
    private static final String ERROR_SCHEMA = "XSD Schema doesn't exist";
    private static final String ERROR_DEFAULT_XML = "Default XML doesn't exist";
    private static final String ERROR_FULL_XML = "Full XML doesn't exist";
    private static final String ERROR_GRID_XML = "Grid XML doesn't exist";
    private static final String ERROR_SERVICES_XML = "Services XML doesn't exist";
    private static final String ERROR_ROCCI_XML = "Rocci XML doesn't exist";
    private static final String ERROR_JCLOUDS_XML = "JClouds XML doesn't exist";

    // Test Logger
    private static final Logger logger = LogManager.getLogger("Console");


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

    @AfterClass
    public static void afterClass() throws Exception {
        if ((new File(XML_TMP_PATH)).exists()) {
            (new File(XML_TMP_PATH)).delete();
        }
    }

    /*
     * *************************************************************** 
     * CONSTRUCTOR TESTS
     ***************************************************************/
    @Test
    public void creation_XMLfile_XSDschema() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd, logger);

        // Checkers
        assertNotNull(project);

        int numWorkers_byName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkers_byName, 1);

        int numWorkers_byList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkers_byList, 1);

        int numWorkers_byHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkers_byHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDschema() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        String defaultProject = buildXMLString();

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd, logger);

        // Checkers
        assertNotNull(project);

        int numWorkers_byName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkers_byName, 1);

        int numWorkers_byList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkers_byList, 1);

        int numWorkers_byHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkers_byHashMap, 1);
    }

    @Test
    public void creation_XMLfile_XSDpath() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        int numWorkers_byName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkers_byName, 1);

        int numWorkers_byList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkers_byList, 1);

        int numWorkers_byHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkers_byHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDpath() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        String defaultProject = buildXMLString();

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        int numWorkers_byName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkers_byName, 1);

        int numWorkers_byList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkers_byList, 1);

        int numWorkers_byHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkers_byHashMap, 1);
    }

    /*
     * *************************************************************** 
     * Dumpers checkers
     ***************************************************************/
    @Test
    public void XMLtoFile() throws URISyntaxException, SAXException, JAXBException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(defaultProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void XMLtoString() throws URISyntaxException, SAXException, JAXBException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        String real_xml = project.getString();
        String ideal_xml = buildXMLString();
        assertEquals("To string method doesn't produce the correct dump", real_xml, ideal_xml);
    }

    /*
     * ***************************************************************
     * Different types of XML checkers
     ***************************************************************/
    @Test
    public void fullXML() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File fullProject = new File(FULL_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(fullProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(fullProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void gridXML() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gridProject = new File(GRID_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(gridProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gridProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void servicesXML() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File servicesProject = new File(SERVICES_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(servicesProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(servicesProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void rocciXML() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File rocciProject = new File(ROCCI_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(rocciProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(rocciProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void jclouds_gceXML() throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsd_path = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gceProject = new File(JCLOUDS_GCE_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(gceProject, xsd_path, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gceProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    /*
     * ***************************************************************
     * XML String builder (PRIVATE)
     ***************************************************************/
    private String buildXMLString() {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<Project>").append("\n");
        sb.append("    ").append("<MasterNode/>").append("\n");

        sb.append("    ").append("<ComputeNode Name=\"localhost\">").append("\n");
        sb.append("    ").append("    ").append("<InstallDir>/opt/COMPSs/</InstallDir>").append("\n");
        sb.append("    ").append("    ").append("<WorkingDir>/tmp/COMPSsWorker/</WorkingDir>").append("\n");
        sb.append("    ").append("</ComputeNode>").append("\n");

        sb.append("</Project>").append("\n");

        return sb.toString();
    }

}
