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
package es.bsc.compss.types.project.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import es.bsc.compss.types.project.ProjectFile;
import es.bsc.compss.types.project.exceptions.ProjectFileValidationException;
import es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.project.jaxb.NIOAdaptorProperties;

import jakarta.xml.bind.JAXBException;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

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
     * @throws Exception Error in after class
     */
    @AfterClass
    public static void afterClass() throws Exception {
        if ((new File(XML_TMP_PATH)).exists()) {
            (new File(XML_TMP_PATH)).delete();
        }
    }

    /* ******** CONSTRUCTOR TESTS **********/

    @Test
    public void creation_XMLfile_XSDschema()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd, logger);

        // Checkers
        assertNotNull(project);

        int numWorkersByName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDschema()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Instantiate XSD Schema
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Schema xsd = sf.newSchema(new File(SCHEMA_PATH));

        // Get default resources file
        String defaultProject = buildXMLString();

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsd, logger);

        // Checkers
        assertNotNull(project);

        int numWorkersByName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLfile_XSDpath()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        int numWorkersByName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    @Test
    public void creation_XMLstring_XSDpath()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        String defaultProject = buildXMLString();

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        int numWorkersByName = project.getComputeNodes_names().size();
        assertEquals("Should have a worker", numWorkersByName, 1);

        int numWorkersByList = project.getComputeNodes_list().size();
        assertEquals("Should have a worker", numWorkersByList, 1);

        int numWorkersByHashMap = project.getComputeNodes_hashMap().size();
        assertEquals("Should have a worker", numWorkersByHashMap, 1);
    }

    /* ********** Dumpers checkers *************/
    @Test
    public void xmlToFile()
        throws URISyntaxException, SAXException, JAXBException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(defaultProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void xmlToString() throws URISyntaxException, SAXException, JAXBException, ProjectFileValidationException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File defaultProject = new File(DEFAULT_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(defaultProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        String realXml = project.getString();
        String idealXml = buildXMLString();
        assertEquals("To string method doesn't produce the correct dump", realXml, idealXml);
    }

    /*
     * *************************************************************** Different types of XML checkers
     ***************************************************************/
    @Test
    public void fullXML()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File fullProject = new File(FULL_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(fullProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        Map<String, Object> props =
            project.getAdaptorProperties(project.getComputeNode("CN_full"), "es.bsc.compss.nio.master.NIOAdaptor");
        assertNotNull(props);
        assertEquals(NIOAdaptorProperties.class, props.get("Ports").getClass());
        assertEquals(ExternalAdaptorProperties.class, props.get("Properties").getClass());
        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(fullProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void gridXML()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gridProject = new File(GRID_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(gridProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gridProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void servicesXML()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File servicesProject = new File(SERVICES_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(servicesProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(servicesProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void rocciXML()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File rocciProject = new File(ROCCI_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(rocciProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(rocciProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    @Test
    public void jclouds_gceXML()
        throws URISyntaxException, JAXBException, SAXException, ProjectFileValidationException, IOException {
        // Get XSD Schema path
        String xsdPath = new File(SCHEMA_PATH).toURI().getPath();

        // Get default resources file
        File gceProject = new File(JCLOUDS_GCE_XML_PATH);

        // Instantiate ResourcesFile
        ProjectFile project = new ProjectFile(gceProject, xsdPath, logger);

        // Checkers
        assertNotNull(project);

        File xml = new File(XML_TMP_PATH);
        project.toFile(xml);
        boolean compareFile = FileUtils.contentEquals(gceProject, xml);
        assertEquals("Dump content not equal", true, compareFile);
    }

    /* ********** XML String builder (PRIVATE) ************/
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

    /* ********** XML String builder (PRIVATE) ************/
    private String buildXMLString(String environmentScript) {
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n");
        sb.append("<Project>").append("\n");
        sb.append("    ").append("<MasterNode/>").append("\n");

        sb.append("    ").append("<ComputeNode Name=\"localhost\">").append("\n");
        sb.append("    ").append("    ").append("<InstallDir>/opt/COMPSs/</InstallDir>").append("\n");
        sb.append("    ").append("    ").append("<Application>").append("\n");
        sb.append("    ").append("    ").append("<EnvironmentScript>").append(environmentScript)
            .append("</EnvironmentScript>").append("\n");
        sb.append("    ").append("    ").append("</Application>").append("\n");
        sb.append("    ").append("</ComputeNode>").append("\n");

        sb.append("</Project>").append("\n");

        return sb.toString();
    }

}
