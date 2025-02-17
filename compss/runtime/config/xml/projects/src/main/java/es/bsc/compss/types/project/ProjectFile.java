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
package es.bsc.compss.types.project;

import es.bsc.compss.types.project.exceptions.InvalidElementException;
import es.bsc.compss.types.project.exceptions.ProjectFileValidationException;
import es.bsc.compss.types.project.jaxb.AdaptorType;
import es.bsc.compss.types.project.jaxb.AdaptorsListType;
import es.bsc.compss.types.project.jaxb.ApplicationType;
import es.bsc.compss.types.project.jaxb.BatchType;
import es.bsc.compss.types.project.jaxb.CloudPropertiesType;
import es.bsc.compss.types.project.jaxb.CloudPropertyType;
import es.bsc.compss.types.project.jaxb.CloudProviderType;
import es.bsc.compss.types.project.jaxb.CloudType;
import es.bsc.compss.types.project.jaxb.ClusterNodeType;
import es.bsc.compss.types.project.jaxb.ComputeNodeType;
import es.bsc.compss.types.project.jaxb.ComputingClusterType;
import es.bsc.compss.types.project.jaxb.DataNodeType;
import es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.project.jaxb.GOSAdaptorProperties;
import es.bsc.compss.types.project.jaxb.HttpType;
import es.bsc.compss.types.project.jaxb.ImageType;
import es.bsc.compss.types.project.jaxb.ImagesType;
import es.bsc.compss.types.project.jaxb.InstanceTypeType;
import es.bsc.compss.types.project.jaxb.InstanceTypesType;
import es.bsc.compss.types.project.jaxb.InteractiveType;
import es.bsc.compss.types.project.jaxb.MasterNodeType;
import es.bsc.compss.types.project.jaxb.MemoryType;
import es.bsc.compss.types.project.jaxb.NIOAdaptorProperties;
import es.bsc.compss.types.project.jaxb.OSType;
import es.bsc.compss.types.project.jaxb.OSTypeType;
import es.bsc.compss.types.project.jaxb.ObjectFactory;
import es.bsc.compss.types.project.jaxb.PackageType;
import es.bsc.compss.types.project.jaxb.ProcessorPropertyType;
import es.bsc.compss.types.project.jaxb.ProcessorType;
import es.bsc.compss.types.project.jaxb.ProjectType;
import es.bsc.compss.types.project.jaxb.PropertyAdaptorType;
import es.bsc.compss.types.project.jaxb.ServiceType;
import es.bsc.compss.types.project.jaxb.SoftwareListType;
import es.bsc.compss.types.project.jaxb.StorageType;
import es.bsc.compss.types.project.jaxb.SubmissionSystemType;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;

import java.io.File;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;


public class ProjectFile {

    public static final String PORTS = "Ports";
    public static final String BROKER_ADAPTOR = "BrokerAdaptor";
    public static final String PROPERTIES = "Properties";
    private static final String BATCH_PROPERTIES = "BatchProperties";

    // JAXB context
    private JAXBContext context;

    // XSD Schema
    private Schema xsd;

    // Resources instance
    private ProjectType project;

    // Associated validator
    private Validator validator;

    // Logger
    private Logger logger;


    /* ************* CONSTRUCTORS ***************/
    /**
     * Creates a ProjectFile instance from a given XML file. The XML is validated against the given schema and
     * internally validated
     *
     * @param xml Project xml file
     * @param xsd Project schema
     * @param logger Logger to write output
     * @throws JAXBException Error occurs during Exception
     * @throws ProjectFileValidationException Error occurs during validation
     */
    public ProjectFile(File xml, Schema xsd, Logger logger) throws JAXBException, ProjectFileValidationException {
        this.logger = logger;
        this.logger.info("Init Project.xml parsing");
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        this.xsd = xsd;

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(xml);
        this.project = (ProjectType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Project.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Project.xml finished");
    }

    /**
     * Creates a ProjectFile instance from a given XML string. The XML is validated against the given schema and
     * internally validated
     *
     * @param xmlString Project XML document as file
     * @param xsd Project XML schema
     * @param logger Logger to write output
     * @throws JAXBException Error occurs during parsing
     * @throws ProjectFileValidationException Error occurs during validation
     */
    public ProjectFile(String xmlString, Schema xsd, Logger logger)
        throws JAXBException, ProjectFileValidationException {
        this.logger = logger;
        this.logger.info("Init Project.xml parsing");
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        this.xsd = xsd;

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(new StringReader(xmlString));
        this.project = (ProjectType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Project.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Project.xml finished");
    }

    /**
     * Creates a ProjectFile instance from a given XML file. The XML is validated against the given path of the schema
     * and internally validated
     *
     * @param xml Project XML File
     * @param xsdPath Location to Project XML schema
     * @param logger Logger to write output
     * @throws SAXException Error occurs during parsing
     * @throws JAXBException Error occurs during parsing
     * @throws ProjectFileValidationException Error occurs during validation
     */
    public ProjectFile(File xml, String xsdPath, Logger logger)
        throws SAXException, JAXBException, ProjectFileValidationException {
        this.logger = logger;
        this.logger.info("Init Project.xml parsing");
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(xml);
        this.project = (ProjectType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Project.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Project.xml finished");
    }

    /**
     * Creates a ProjectFile instance from a given XML string. The XML is validated against the given path of the schema
     * and internally validated
     *
     * @param xmlString Project XML document as string
     * @param xsdPath Project XML schema location
     * @param logger Logger to write output
     * @throws SAXException Error occurs during parsing
     * @throws JAXBException Error occurs during parsing
     * @throws ProjectFileValidationException Error occurs during validation
     */
    public ProjectFile(String xmlString, String xsdPath, Logger logger)
        throws SAXException, JAXBException, ProjectFileValidationException {
        this.logger = logger;
        this.logger.info("Init Project.xml parsing");
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(new StringReader(xmlString));
        this.project = (ProjectType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Project.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Project.xml finished");
    }

    /**
     * Creates a ProjectFile instance with empty values.
     *
     * @param xsdPath Project XML schema location
     * @param logger Logger to write output
     * @throws SAXException Error occurs during parsing
     * @throws JAXBException Error occurs during parsing
     * @throws ProjectFileValidationException Error occurs during validation
     */
    public ProjectFile(String xsdPath, Logger logger)
        throws SAXException, JAXBException, ProjectFileValidationException {
        this.logger = logger;
        this.logger.info("Init Project.xml parsing");
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        this.project = new ProjectType();
        addEmptyMaster();

        // Validate
        this.logger.info("Init Project.xml validation");
        validator = new Validator(this, this.logger);
        this.logger.info("Project.xml finished");
    }

    /**
     * Creates an empty ProjectFile.
     * 
     * @param logger Logger to write output
     * @throws JAXBException Error occurs during parsing
     */
    public ProjectFile(Logger logger) throws JAXBException {
        this.logger = logger;
        this.project = new ProjectType();
        this.validator = new Validator(this, this.logger);
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        addEmptyMaster();
    }

    private void addEmptyMaster() {
        MasterNodeType master = new MasterNodeType();
        project.getMasterNodeOrComputeNodeOrDataNode().add(master);

    }

    /* ************** DUMPERS ****************/

    /**
     * Stores the current Project object to the given file.
     *
     * @param file file to dump the XML project file
     * @throws JAXBException Error occurs during JAXB operation
     */
    public void toFile(File file) throws JAXBException {
        logger.info("Project.xml to file");
        Marshaller m = this.context.createMarshaller();
        ObjectFactory objFact = new ObjectFactory();

        m.setSchema(this.xsd);
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        m.marshal(objFact.createProject(this.project), file);
    }

    /**
     * Returns the string construction of the current Project.
     *
     * @return project XML as string
     * @throws JAXBException Error occurs during JAXB operation
     */
    public String getString() throws JAXBException {
        logger.info("Project.xml to string");
        Marshaller m = this.context.createMarshaller();
        ObjectFactory objFact = new ObjectFactory();

        m.setSchema(this.xsd);
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        StringWriter sw = new StringWriter();
        m.marshal(objFact.createProject(this.project), sw);
        return sw.getBuffer().toString();
    }

    /* ***************** GETTERS: MAIN ELEMENTS LISTS *******************/

    /**
     * Returns the JAXB class representing the all XML file content.
     *
     * @return project object
     */
    public ProjectType getProject() {
        return this.project;
    }

    /**
     * Get the MasterNode. Null if not found (but XSD schema doesn't allow it)
     *
     * @return master object
     */
    public MasterNodeType getMasterNode() {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MasterNodeType) {
                    return ((MasterNodeType) obj);
                }
            }
        }

        return null;
    }

    /**
     * Get the cloud description. Null if not found (but XSD schema doesn't allow it)
     * 
     * @return Cloud object
     */
    public CloudType getCloud() {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    return ((CloudType) obj);
                }
            }
        }

        return null;
    }

    /**
     * Get a list of declared ComputeNodes.
     *
     * @return Compute node objects list
     */
    public List<ComputeNodeType> getComputeNodes_list() {
        ArrayList<ComputeNodeType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    list.add((ComputeNodeType) obj);
                }
            }
        }

        return list;
    }

    /**
     * Get a list of declared DataNodes.
     *
     * @return Data node objects list
     */
    public List<DataNodeType> getDataNodes_list() {
        ArrayList<DataNodeType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof DataNodeType) {
                    list.add((DataNodeType) obj);
                }
            }
        }

        return list;
    }

    /**
     * Get a list of declared Services.
     *
     * @return Service objects list
     */
    public List<ServiceType> getServices_list() {
        ArrayList<ServiceType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    list.add((ServiceType) obj);
                }
            }
        }

        return list;
    }

    /**
     * Get a list of declared HTTP Services.
     *
     * @return HTTP Service objects list
     */
    public List<HttpType> getHttpServices_list() {
        ArrayList<HttpType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof HttpType) {
                    list.add((HttpType) obj);
                }
            }
        }

        return list;
    }

    /**
     * Returns a list of declared CloudProviders.
     *
     * @return Cloud providers object lists
     */
    public List<CloudProviderType> getCloudProviders_list() {
        ArrayList<CloudProviderType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    CloudType cloud = (CloudType) obj;
                    List<JAXBElement<?>> cloudPropsList = cloud.getCloudProviderOrInitialVMsOrMinimumVMs();
                    if (cloudPropsList != null) {
                        for (JAXBElement<?> jaxbelem : cloudPropsList) {
                            if (jaxbelem.getName().equals(new QName("CloudProvider"))) {
                                list.add((CloudProviderType) jaxbelem.getValue());
                            }
                        }
                    }
                }
            }
        }

        return list;
    }

    /**
     * Returns a list of declared ComputingCluster.
     *
     * @return Computing cluster object lists
     */
    public List<ComputingClusterType> getComputingCluster_list() {
        ArrayList<ComputingClusterType> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputingClusterType) {
                    list.add((ComputingClusterType) obj);
                }
            }
        }

        return list;
    }

    /*
     * ************************************** GETTERS: MAIN ELEMENTS HASH-MAPS
     **************************************/
    /**
     * Returns the number of Compunting Units of a given processor @p.
     *
     * @param p Processor object
     * @return return number of computing units in the processor
     */
    public int getProcessorComputingUnits(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("ComputingUnits"))) {
                    return (Integer) obj.getValue();
                }
            }
        }

        return -1;
    }

    /**
     * Returns the architecture of a given processor @p.
     *
     * @param p Processor object
     * @return architecture
     */
    public String getProcessorArchitecture(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Architecture"))) {
                    return (String) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the speed of a given processor @p.
     *
     * @param p Processor object
     * @return speed
     */
    public float getProcessorSpeed(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Speed"))) {
                    return (Float) obj.getValue();
                }
            }
        }
        return (float) -1.0;
    }

    /**
     * Returns the type of a given processor @p.
     *
     * @param p Processor Object
     * @return object
     */
    public String getProcessorType(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Type"))) {
                    return (String) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the internal memory of a given processor @p.
     *
     * @param p Processor object
     * @return internal memory
     */
    public float getProcessorMemorySize(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("InternalMemorySize"))) {
                    return (Float) obj.getValue();
                }
            }
        }
        return (float) -1.0;
    }

    /**
     * Returns the processor property of a given processor @p.
     *
     * @param p Processor object
     * @return property value
     */
    public ProcessorPropertyType getProcessorProperty(ProcessorType p) {
        List<JAXBElement<?>> objList = p.getComputingUnitsOrArchitectureOrSpeed();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("ProcessorProperty"))) {
                    return (ProcessorPropertyType) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Get the size of a given memory @memNode.
     * 
     * @param memNode memObject
     * @return size
     */
    public float getMemorySize(MemoryType memNode) {
        List<Serializable> memProps = memNode.getSizeOrType();
        if (memProps != null) {
            for (Serializable prop : memProps) {
                if (prop instanceof Float) {
                    return (float) prop;
                }
            }
        }
        return (float) -1.0;
    }

    /**
     * Get the type of a given memory @memNode.
     * 
     * @param memNode Memory Object
     * @return type
     */
    public String getMemoryType(MemoryType memNode) {
        List<Serializable> memProps = memNode.getSizeOrType();
        if (memProps != null) {
            for (Serializable prop : memProps) {
                if (prop instanceof String) {
                    return (String) prop;
                }
            }
        }
        return (String) null;
    }

    /**
     * Get the size of a given storage @strNode.
     * 
     * @param strNode Storage Object
     * @return size
     */
    public float getStorageSize(StorageType strNode) {
        List<Serializable> storageProps = strNode.getSizeOrTypeOrBandwidth();
        if (storageProps != null) {
            for (Serializable prop : storageProps) {
                if (prop instanceof Float) {
                    return (float) prop;
                }
            }
        }
        return (float) -1.0;
    }

    /**
     * Get the type of a given storage @strNode.
     * 
     * @param strNode Storage Object
     * @return size
     */
    public String getStorageType(StorageType strNode) {
        List<Serializable> storageProps = strNode.getSizeOrTypeOrBandwidth();
        if (storageProps != null) {
            for (Serializable prop : storageProps) {
                if (prop instanceof String) {
                    return (String) prop;
                }
            }
        }
        return null;
    }

    /**
     * Get the bandwidth of a given storage @strNode.
     * 
     * @param strNode Storage Object
     * @return bandwidth
     */
    public int getStorageBW(StorageType strNode) {
        List<Serializable> storageProps = strNode.getSizeOrTypeOrBandwidth();
        if (storageProps != null) {
            for (Serializable prop : storageProps) {
                if (prop instanceof Integer) {
                    return (Integer) prop;
                }
            }
        }
        return -1;
    }

    /**
     * Returns the type of a given OS @os.
     *
     * @param os Operating System object
     * @return Type
     */
    public String getOperatingSystemType(OSType os) {
        List<JAXBElement<?>> innerElements = os.getTypeOrDistributionOrVersion();
        if (innerElements != null) {
            for (JAXBElement<?> elem : innerElements) {
                if (elem.getName().equals(new QName("Type"))) {
                    OSTypeType osType = (OSTypeType) elem.getValue();
                    return osType.value();
                }
            }
        }
        return null;
    }

    /**
     * Returns the distribution of a given OS @os.
     *
     * @param os Operating System object
     * @return distribution
     */
    public String getOperatingSystemDistribution(OSType os) {
        List<JAXBElement<?>> innerElements = os.getTypeOrDistributionOrVersion();
        if (innerElements != null) {
            for (JAXBElement<?> elem : innerElements) {
                if (elem.getName().equals(new QName("Distribution"))) {
                    return (String) elem.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Returns the OS Version.
     *
     * @param os OS object
     * @return version
     */
    public String getOperatingSystemVersion(OSType os) {
        List<JAXBElement<?>> innerElements = os.getTypeOrDistributionOrVersion();
        if (innerElements != null) {
            for (JAXBElement<?> elem : innerElements) {
                if (elem.getName().equals(new QName("Version"))) {
                    return (String) elem.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Returns a HashMap of declared ComputeNodes (Key: Name, Value: CN).
     *
     * @return Computing Nodes map
     */
    public HashMap<String, ComputeNodeType> getComputeNodes_hashMap() {
        HashMap<String, ComputeNodeType> res = new HashMap<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    String name = ((ComputeNodeType) obj).getName();
                    res.put(name, (ComputeNodeType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Returns a HashMap of declared DataNodes (Key: Name, Value: DN).
     *
     * @return DataNodes map
     */
    public HashMap<String, DataNodeType> getDataNodes_hashMap() {
        HashMap<String, DataNodeType> res = new HashMap<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof DataNodeType) {
                    String name = ((DataNodeType) obj).getName();
                    res.put(name, (DataNodeType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Returns a HashMap of declared Services (Key: WSDL, Value: Service).
     *
     * @return
     */
    public HashMap<String, ServiceType> getServices_hashMap() {
        HashMap<String, ServiceType> res = new HashMap<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    String serviceWSDL = ((ServiceType) obj).getWsdl();
                    res.put(serviceWSDL, (ServiceType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Returns a HashMap of declared CloudProviders (Key: Name, Value: CP).
     *
     * @return
     */
    public HashMap<String, CloudProviderType> getCloudProviders_hashMap() {
        HashMap<String, CloudProviderType> res = new HashMap<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    CloudType cloud = (CloudType) obj;
                    List<JAXBElement<?>> cloudPropsList = cloud.getCloudProviderOrInitialVMsOrMinimumVMs();
                    if (cloudPropsList != null) {
                        for (JAXBElement<?> jaxbelem : cloudPropsList) {
                            if (jaxbelem.getName().equals(new QName("CloudProvider"))) {
                                String providerName = ((CloudProviderType) jaxbelem.getValue()).getName();
                                res.put(providerName, ((CloudProviderType) jaxbelem.getValue()));
                            }
                        }
                    }
                }
            }
        }

        return res;
    }

    /* ********* GETTERS: MAIN ELEMENTS KEY-VALUES (NAME) **********/

    /**
     * Returns a List of the names of the declared ComputeNodes.
     *
     * @return
     */
    public List<String> getComputeNodes_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    list.add(((ComputeNodeType) obj).getName());
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the names of the declared DataNodes.
     *
     * @return
     */
    public List<String> getDataNodes_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof DataNodeType) {
                    list.add(((DataNodeType) obj).getName());
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the names of the declared Services.
     *
     * @return
     */
    public List<String> getServices_wsdls() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    list.add(((ServiceType) obj).getWsdl());
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the names of the declared CloudProviders.
     *
     * @return List of Cloud Providers
     */
    public List<String> getCloudProviders_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    CloudType cloud = (CloudType) obj;
                    List<JAXBElement<?>> cloudPropsList = cloud.getCloudProviderOrInitialVMsOrMinimumVMs();
                    if (cloudPropsList != null) {
                        for (JAXBElement<?> jaxbelem : cloudPropsList) {
                            if (jaxbelem.getName().equals(new QName("CloudProvider"))) {
                                String providerName = ((CloudProviderType) jaxbelem.getValue()).getName();
                                list.add(providerName);
                            }
                        }
                    }
                }
            }
        }

        return list;
    }

    /* ******** GETTERS: MAIN ELEMENTS SINGLE **********/

    /**
     * Returns the ComputeNode with name = @name.
     *
     * @param name Compute node name
     * @return Compute node Object. Null if name doesn't exist
     */
    public ComputeNodeType getComputeNode(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    ComputeNodeType cn = (ComputeNodeType) obj;
                    if (cn.getName().equals(name)) {
                        return cn;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the user of a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @return Username. Null if not defined
     */
    public String getUser(ComputeNodeType cn) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("User"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Returns the user of a given ComputingCluster @cn.
     *
     * @param cn Computing cluster object
     * @return Username. Null if not defined
     */
    public String getUser(ComputingClusterType cn) {
        List<JAXBElement<?>> elementList = cn.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("User"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Get the defined username in a given Image @image.
     * 
     * @param image Image object.
     * @return Username. Null if not defined.
     */
    public String getUser(ImageType image) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("User"))) {
                    return (String) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the installDir of a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @return Installation directory. Null if not defined.
     */
    public String getInstallDir(ComputeNodeType cn) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("InstallDir"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Returns the installDir of a given ComputingClusterType @cn.
     *
     * @param cn Computing Cluster object
     * @return Installation directory. Null if not defined.
     */
    public String getInstallDir(ComputingClusterType cn) {
        List<JAXBElement<?>> elementList = cn.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("InstallDir"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Get the defined installation directory in a given Image @image.
     * 
     * @param image Image object.
     * @return Installation directory. Null if not defined.
     */
    public String getInstallDir(ImageType image) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("InstallDir"))) {
                    return (String) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Get the defined working directory in a given Image @image.
     * 
     * @param image Image object.
     * @return Working directory. Null if not defined.
     */
    public String getWorkingDir(ImageType image) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("WorkingDir"))) {
                    return (String) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the workingDir of a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @return Working directory. Null if not defined.
     */
    public String getWorkingDir(ComputeNodeType cn) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("WorkingDir"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Returns the workingDir of a given ComputingCluster @cn.
     *
     * @param cn Computing Cluster object
     * @return Working directory. Null if not defined.
     */
    public String getWorkingDir(ComputingClusterType cn) {
        List<JAXBElement<?>> elementList = cn.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("WorkingDir"))) {
                    return ((String) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Get the defined limit of tasks in a given Image @image.
     * 
     * @param image Image object.
     * @return Limit of tasks. Null if not defined.
     */
    public int getLimitOfTasks(ImageType image) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("LimitOfTasks"))) {
                    return (Integer) obj.getValue();
                }
            }
        }

        return -1;
    }

    /**
     * Returns the limitOfTasks of a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @return Limit of tasks. -1 if not defined.
     */
    public int getLimitOfTasks(ComputeNodeType cn) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("LimitOfTasks"))) {
                    return ((Integer) element.getValue());
                }
            }
        }

        return -1;
    }

    /**
     * Returns the limitOfTasks of a given ComputingCluster @cn.
     *
     * @param cn Computing Cluster object
     * @return Limit of tasks. -1 if not defined.
     */
    public int getLimitOfTasks(ComputingClusterType cn) {
        List<JAXBElement<?>> elementList = cn.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("LimitOfTasks"))) {
                    return ((Integer) element.getValue());
                }
            }
        }

        return -1;
    }

    /**
     * Get the defined Application description in a given Image @image.
     * 
     * @param image Image object.
     * @return Application description object. Null if not found.
     */
    public ApplicationType getApplication(ImageType image) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Application"))) {
                    return (ApplicationType) obj.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the application information of a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @return Null if not defined
     */
    public ApplicationType getApplication(ComputeNodeType cn) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("Application"))) {
                    return ((ApplicationType) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Returns the application information of a given Computing Cluster @cn.
     *
     * @param cn Computing cluster object
     * @return Null if not defined
     */
    public ApplicationType getApplication(ComputingClusterType cn) {
        List<JAXBElement<?>> elementList = cn.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("Application"))) {
                    return ((ApplicationType) element.getValue());
                }
            }
        }

        return null;
    }

    /**
     * Returns the queues of a given Adaptor @adaptorName within a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @param adaptorName Name of the COMPSs adaptor
     * @return List of defined queues for an adaptor. Null if not found
     */
    public List<String> getAdaptorQueues(ComputeNodeType cn, String adaptorName) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            // Loop for adaptors tag
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("Adaptors"))) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) element.getValue()).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return getAdaptorQueues(adaptor);
                            }
                        }
                    } else {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Get the queues of a given Adaptor @adaptorName within a given Image @image.
     *
     * @param image Image object.
     * @param adaptorName Adaptor name.
     * @return List of queue names. Empty list if not found
     */
    public List<String> getAdaptorQueues(ImageType image, String adaptorName) {
        List<String> adaptorQueues = new ArrayList<>();

        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            // Loop for adaptors tag
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Adaptors"))) {
                    AdaptorsListType adaptorsList = ((AdaptorsListType) obj.getValue());
                    if (adaptorsList != null) {
                        List<AdaptorType> adaptors = adaptorsList.getAdaptor();
                        if (adaptors != null) {
                            // Loop for specific adaptor name
                            for (AdaptorType adaptor : adaptors) {
                                if (adaptor.getName().equals(adaptorName)) {
                                    return getAdaptorQueues(adaptor);
                                }
                            }
                        } else {
                            return adaptorQueues; // Empty
                        }
                    } else {
                        return adaptorQueues; // Empty
                    }
                }
            }
        }

        return adaptorQueues; // Empty
    }

    /**
     * Returns the adaptor queues in a given Adaptor @adaptor.
     *
     * @param adaptor Adaptor description object.
     * @return List of queue names. Empty list if not found.
     */
    public List<String> getAdaptorQueues(AdaptorType adaptor) {
        List<String> adaptorQueues = new ArrayList<>();

        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            // Loop for submission system
            for (JAXBElement<?> adaptorElement : innerElements) {
                if (adaptorElement.getName().equals(new QName("SubmissionSystem"))) {
                    SubmissionSystemType subSys = (SubmissionSystemType) adaptorElement.getValue();
                    List<Object> subSysTypes = subSys.getBatchOrInteractive();
                    if (subSysTypes != null) {
                        // Loop for BATCH
                        for (Object subSysType : subSysTypes) {
                            if (subSysType instanceof BatchType) {
                                // Get declared queues
                                List<String> queues = ((BatchType) subSysType).getQueue();
                                if (queues != null) {
                                    return queues;
                                } else {
                                    return adaptorQueues; // Empty
                                }
                            }
                        }
                    } else {
                        return adaptorQueues; // Empty
                    }
                }
            }
        }

        return adaptorQueues; // Empty
    }

    /**
     * Returns the adaptor batch properties in a given Adaptor @adaptor.
     *
     * @param adaptor Adaptor description object.
     * @return HashMap with the batchProperties. Empty list if not found.
     */
    public HashMap<String, Object> getQueuesAndBatchProperties(AdaptorType adaptor) {
        HashMap<String, Object> adaptorBatchProperties = new HashMap<>();
        adaptorBatchProperties.put("Interactive", false);
        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            for (JAXBElement<?> adaptorElement : innerElements) {
                if (adaptorElement.getName().equals(new QName("SubmissionSystem"))) {
                    SubmissionSystemType subSys = (SubmissionSystemType) adaptorElement.getValue();
                    List<Object> subSysTypes = subSys.getBatchOrInteractive();
                    if (subSysTypes != null) {
                        // Loop for BATCH
                        for (Object subSysType : subSysTypes) {
                            if (subSysType instanceof InteractiveType) {
                                // IF is interactive, set interactive to true
                                adaptorBatchProperties.put("Interactive", true);
                            }
                            if (subSysType instanceof BatchType) {
                                GOSAdaptorProperties properties = ((BatchType) subSysType).getBatchProperties();
                                adaptorBatchProperties.put("Queue", ((BatchType) subSysType).getQueue());
                                adaptorBatchProperties.put("Port", properties.getPort());
                                adaptorBatchProperties.put("MaxExecTime", properties.getMaxExecTime());
                                adaptorBatchProperties.put("QOS", properties.getQOS());
                                adaptorBatchProperties.put("Reservation", properties.getReservation());
                                adaptorBatchProperties.put("FileCFG", properties.getFileCFG());
                                adaptorBatchProperties.put("ProjectName", properties.getProjectName());
                            }
                        }
                    }
                }
            }
        }
        return adaptorBatchProperties;
    }

    /**
     * Returns the declared properties of a given Adaptor @adaptorName within a given ComputeNode @cn.
     *
     * @param cn Compute Node object
     * @param adaptorName Name of the COMPSs adaptor
     * @return Declared properties. Null if not found
     */
    public Map<String, Object> getAdaptorProperties(ComputeNodeType cn, String adaptorName) {
        List<JAXBElement<?>> elementList = cn.getInstallDirOrWorkingDirOrUser();
        if (elementList != null) {
            // Loop for adaptors tag
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("Adaptors"))) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) element.getValue()).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return getAdaptorProperties(adaptor);
                            }
                        }
                    } else {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Returns the declared properties of a given Adaptor @adaptorName within a given ComputingClusterType @cluster.
     *
     * @param cluster the cluster
     * @param adaptorName the adaptor name
     * @return the adaptor properties
     */
    public Map<String, Object> getAdaptorProperties(ComputingClusterType cluster, String adaptorName) {
        HashMap<String, Object> properties = new HashMap<String, Object>();
        List<JAXBElement<?>> elementList = cluster.getLimitOfTasksOrInstallDirOrWorkingDir();
        if (elementList != null) {
            // Loop for adaptors tag
            for (JAXBElement<?> element : elementList) {
                if (element.getName().equals(new QName("Adaptors"))) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) element.getValue()).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return getAdaptorProperties(adaptor);
                            }
                        }
                    } else {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Returns the defined properties for a given Adaptor description @adaptor.
     *
     * @param adaptor Adaptor description object.
     * @return Properties object. Null if not defined
     */
    public Map<String, Object> getAdaptorProperties(AdaptorType adaptor) {
        HashMap<String, Object> properties = new HashMap<String, Object>();
        if (adaptor.getName().equals("es.bsc.compss.gos.master.GOSAdaptor")) {
            properties = getQueuesAndBatchProperties(adaptor);
        }
        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            // Loop for submission system
            for (JAXBElement<?> adaptorElement : innerElements) {
                if (adaptorElement.getName().equals(new QName(PORTS))
                    || adaptorElement.getName().equals(new QName(BROKER_ADAPTOR))
                    || adaptorElement.getName().equals(new QName(PROPERTIES))) {
                    properties.put(adaptorElement.getName().getLocalPart(), (Object) adaptorElement.getValue());
                }
            }
        }

        return properties;
    }

    /**
     * Get the declared properties of a given Adaptor @adaptorName within a given Image @image.
     *
     * @param image Image object.
     * @param adaptorName Adaptor name
     * @return AdaptorProperty object. Null if not defined.
     */
    public Map<String, Object> getAdaptorProperties(ImageType image, String adaptorName) {
        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            // Loop for adaptors tag
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Adaptors"))) {
                    AdaptorsListType adaptorsList = ((AdaptorsListType) obj.getValue());
                    if (adaptorsList != null) {
                        List<AdaptorType> adaptors = adaptorsList.getAdaptor();
                        if (adaptors != null) {
                            // Loop for specific adaptor name
                            for (AdaptorType adaptor : adaptors) {
                                if (adaptor.getName().equals(adaptorName)) {
                                    return getAdaptorProperties(adaptor);
                                }
                            }
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Returns the DataNode with name = @name.
     *
     * @param name Data node name
     * @return Data Node object. Null if name doesn't exist
     */
    public DataNodeType getDataNode(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof DataNodeType) {
                    DataNodeType dn = (DataNodeType) obj;
                    if (dn.getName().equals(name)) {
                        return dn;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the Service with wsdl = @wsdl.
     *
     * @param wsdl Service wsdl
     * @return Service Object. Null if wsdl doesn't exist.
     */
    public ServiceType getService(String wsdl) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    ServiceType s = (ServiceType) obj;
                    if (s.getWsdl().equals(wsdl)) {
                        return s;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the Cloud Provider with name = @name.
     *
     * @param name Cloud
     * @return Cloud Provider object. Null if name doesn't exist
     */
    public CloudProviderType getCloudProvider(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    CloudType cloud = (CloudType) obj;
                    List<JAXBElement<?>> cloudPropsList = cloud.getCloudProviderOrInitialVMsOrMinimumVMs();
                    if (cloudPropsList != null) {
                        for (JAXBElement<?> jaxbelem : cloudPropsList) {
                            if (jaxbelem.getName().equals(new QName("CloudProvider"))) {
                                CloudProviderType cp = (CloudProviderType) jaxbelem.getValue();
                                if (cp.getName().equals(name)) {
                                    return cp;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the initial number of VMs declared on a given cloud description @c.
     *
     * @param c Cloud Description Object.
     * @return Initial number of VMs. Null if not defined.
     */
    public Integer getInitialVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("InitialVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the minimum number of VMs declared on a given cloud description @c.
     *
     * @param c Cloud Description Object.
     * @return Minimum number of VMs. Null if not defined.
     */
    public Integer getMinVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("MinimumVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the maximum number of VMs declared on a given cloud description @c.
     *
     * @param c Cloud Description Object
     * @return Maximum number of VMs. Null if not defined.
     */
    public Integer getMaxVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("MaximumVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Get the defined Packages descriptions in a given Image @image.
     * 
     * @param image Image object.
     * @return List of Package description objects. Empty list if not found.
     */
    public List<PackageType> getPackages(ImageType image) {
        List<PackageType> packages = new ArrayList<>();

        List<JAXBElement<?>> objList = image.getInstallDirOrWorkingDirOrUser();
        if (objList != null) {
            for (JAXBElement<?> obj : objList) {
                if (obj.getName().equals(new QName("Package"))) {
                    packages.add(((PackageType) obj.getValue()));
                }
            }
        }

        return packages;
    }

    /* *************** ADDERS: MAIN ELEMENTS ****************/
    /**
     * Adds the given ComputeNodeType @cn to the project file.
     *
     * @param cn Compute node description object
     * @return Added Compute node description object
     * @throws InvalidElementException Validation Exception.
     */
    public ComputeNodeType addComputeNode(ComputeNodeType cn) throws InvalidElementException {
        validator.validateComputeNode(cn);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(cn);
        return cn;
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name.
     * @param installDir Installation directory
     * @param workingDir Working directory
     * @param user Username
     * @param app Application description
     * @param limitOfTasks Limit of tasks
     * @param adaptors List of Adaptors
     * @return Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user,
        ApplicationType app, int limitOfTasks, AdaptorsListType adaptors) throws InvalidElementException {

        ComputeNodeType cn = new ComputeNodeType();
        cn.setName(name);

        // Mandatory elements
        JAXBElement<String> installDirJaxb = new JAXBElement<String>(new QName("InstallDir"), String.class, installDir);
        cn.getInstallDirOrWorkingDirOrUser().add(installDirJaxb);
        JAXBElement<String> workingDirJaxb = new JAXBElement<String>(new QName("WorkingDir"), String.class, workingDir);
        cn.getInstallDirOrWorkingDirOrUser().add(workingDirJaxb);

        // Non mandatory elements
        if (user != null) {
            JAXBElement<String> userJaxb = new JAXBElement<String>(new QName("User"), String.class, user);
            cn.getInstallDirOrWorkingDirOrUser().add(userJaxb);
        }
        if (app != null) {
            JAXBElement<ApplicationType> appsJaxb =
                new JAXBElement<ApplicationType>(new QName("Application"), ApplicationType.class, app);
            cn.getInstallDirOrWorkingDirOrUser().add(appsJaxb);
        }
        if (limitOfTasks >= 0) {
            JAXBElement<Integer> limitOfTasksJaxb =
                new JAXBElement<Integer>(new QName("LimitOfTasks"), Integer.class, limitOfTasks);
            cn.getInstallDirOrWorkingDirOrUser().add(limitOfTasksJaxb);
        }
        if (adaptors != null) {
            JAXBElement<AdaptorsListType> adaptorsJaxb =
                new JAXBElement<AdaptorsListType>(new QName("Adaptors"), AdaptorsListType.class, adaptors);
            cn.getInstallDirOrWorkingDirOrUser().add(adaptorsJaxb);
        }

        return addComputeNode(cn);
    }

    /**
     * Adds a new ComputeNode with the given information.
     *
     * @param name Compute node name.
     * @param installDir Installation directory
     * @param workingDir Working directory
     * @param user Username
     * @param limitOfTasks Limit of tasks
     * @return Instance of added Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user,
        int limitOfTasks) throws InvalidElementException {
        return addComputeNode(name, installDir, workingDir, user, null, limitOfTasks, (AdaptorsListType) null);
    }

    /**
     * Adds a new ComputeNode with the given information.
     *
     * @param name Compute node name.
     * @param installDir Installation directory
     * @param workingDir Working directory
     * @param user Username
     * @return Instance of added Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user)
        throws InvalidElementException {
        return addComputeNode(name, installDir, workingDir, user, null, -1, (AdaptorsListType) null);
    }

    /**
     * Adds a new ComputeNode with the given information.
     *
     * @param name Compute node name.
     * @param installDir Installation directory
     * @param workingDir Working directory
     * @param user Username
     * @param app Application description
     * @param limitOfTasks Limit of tasks
     * @param adaptors List of Adaptors
     * @return Instance of added Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user,
        ApplicationType app, int limitOfTasks, List<AdaptorType> adaptors) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return addComputeNode(name, installDir, workingDir, user, app, limitOfTasks, adaptorsList);
    }

    /**
     * Adds a new ComputeNode with the given information.
     *
     * @param name Compute Node name
     * @param installDir COMPSs Installation Directory
     * @param workingDir Working Directory
     * @param user Username
     * @param appDir Application installation directory
     * @param libPath Application library path
     * @param cp Application classpath
     * @param pypath Application python path
     * @param envScript Path to script to set environment variables
     * @param limitOfTasks Limit of tasks
     * @param adaptors List of Adaptors
     * @return Instance of added Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, String appDir,
        String libPath, String cp, String pypath, String envScript, int limitOfTasks, List<AdaptorType> adaptors)
        throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return addComputeNode(name, installDir, workingDir, user, appDir, libPath, cp, pypath, envScript, limitOfTasks,
            adaptorsList);
    }

    /**
     * Adds a new ComputeNode with the given information.
     *
     * @param name Compute Node name
     * @param installDir COMPSs Installation Directory
     * @param workingDir Working Directory
     * @param user Username
     * @param appDir Application installation directory
     * @param libPath Application library path
     * @param cp Application classpath
     * @param pypath Application python path
     * @param envScript Path to script to set environment variables
     * @param limitOfTasks Limit of tasks
     * @param adaptors List of Adaptors
     * @return Instance of added Compute Node description object.
     * @throws InvalidElementException Exception validating input data.
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, String appDir,
        String libPath, String cp, String pypath, String envScript, int limitOfTasks, AdaptorsListType adaptors)
        throws InvalidElementException {

        ApplicationType app = createApplication(appDir, libPath, cp, pypath, envScript);

        return addComputeNode(name, installDir, workingDir, user, app, limitOfTasks, adaptors);
    }

    /**
     * Adds the given DataNode @dn to the project file.
     *
     * @param dn Data Node description object.
     * @return Instance of the added Data node description object.
     * @throws InvalidElementException Error validating input data.
     */
    public DataNodeType addDataNode(DataNodeType dn) throws InvalidElementException {
        validator.validateDataNode(dn);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(dn);
        return dn;
    }

    /**
     * Adds a new DataNode with the given information.
     *
     * @param name Data Node name
     * @return Instance of the added Data node description object.
     * @throws InvalidElementException Error validating input data.
     */
    public DataNodeType addDataNode(String name) throws InvalidElementException {
        DataNodeType dn = new DataNodeType();
        dn.setName(name);

        return addDataNode(dn);
    }

    /**
     * Adds a new DataNode with the given information.
     *
     * @param name Data Node name
     * @param adaptors List of adaptors
     * @return Instance of the added Data node description object.
     * @throws InvalidElementException Error validating input data.
     */
    public DataNodeType addDataNode(String name, AdaptorsListType adaptors) throws InvalidElementException {
        DataNodeType dn = new DataNodeType();
        dn.setName(name);
        dn.getAdaptors().add(adaptors);

        return addDataNode(dn);
    }

    /**
     * Adds a new DataNode with the given information.
     *
     * @param name Data Node name
     * @param adaptors List of adaptors
     * @return Instance of the added Data node description object.
     * @throws InvalidElementException Error validating input data.
     */
    public DataNodeType addDataNode(String name, List<AdaptorType> adaptors) throws InvalidElementException {
        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return addDataNode(name, adaptorsList);
    }

    /**
     * Adds the given Service @s to the project file.
     *
     * @param s Service description object.
     * @return Instance of the added Service description object.
     * @throws InvalidElementException Error validating input data.
     */
    public ServiceType addService(ServiceType s) throws InvalidElementException {
        validator.validateService(s);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(s);
        return s;
    }

    /**
     * Adds a new Service with the given information.
     *
     * @param wsdl Service WSDL
     * @return Instance of the added Service description object.
     * @throws InvalidElementException Error validating input data.
     */
    public ServiceType addService(String wsdl) throws InvalidElementException {
        ServiceType s = new ServiceType();
        s.setWsdl(wsdl);

        return addService(s);
    }

    /**
     * Adds a new Service with the given information.
     *
     * @param wsdl Service WSDL
     * @param limitOfTasks Limit of Tasks
     * @return Instance of the added Service description object.
     * @throws InvalidElementException Error validating input data.
     */
    public ServiceType addService(String wsdl, int limitOfTasks) throws InvalidElementException {
        ServiceType s = new ServiceType();
        // Mandatory parameters
        s.setWsdl(wsdl);

        // Optional parameters
        if (limitOfTasks >= 0) {
            s.setLimitOfTasks(limitOfTasks);
        }

        return addService(s);
    }

    /**
     * Adds the given Cloud @c to the project file.
     *
     * @param c Cloud description object.
     * @return Instance of the added cloud description object.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudType addCloud(CloudType c) throws InvalidElementException {
        validator.validateCloud(c);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(c);
        return c;
    }

    /**
     * Adds a new Cloud with the given information.
     *
     * @param cps List of Cloud provider descriptions
     * @return Instance of the added cloud description object.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudType addCloud(List<CloudProviderType> cps) throws InvalidElementException {
        CloudType c = new CloudType();

        // Mandatory elements
        if (cps != null) {
            for (CloudProviderType cp : cps) {
                JAXBElement<CloudProviderType> cpJaxb =
                    new JAXBElement<CloudProviderType>(new QName("CloudProvider"), CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cpJaxb);
            }
        }

        return addCloud(c);
    }

    /**
     * Adds a new Cloud with the given information.
     *
     * @param cps List of Cloud provider descriptions
     * @param initialVMs Number of initial VMs
     * @param minVMs Minimum number of VMs
     * @param maxVMs Maximum number of VMs
     * @return Instance of the added cloud description object.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudType addCloud(List<CloudProviderType> cps, int initialVMs, int minVMs, int maxVMs)
        throws InvalidElementException {
        CloudType c = new CloudType();

        // Mandatory elements
        if (cps != null) {
            for (CloudProviderType cp : cps) {
                JAXBElement<CloudProviderType> cpJaxb =
                    new JAXBElement<CloudProviderType>(new QName("CloudProvider"), CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cpJaxb);
            }
        }

        // Optional parameters
        if (initialVMs >= 0) {
            JAXBElement<Integer> initialVMsJaxb =
                new JAXBElement<Integer>(new QName("InitialVMs"), Integer.class, initialVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(initialVMsJaxb);
        }
        if (minVMs >= 0) {
            JAXBElement<Integer> minVMsJaxb = new JAXBElement<Integer>(new QName("MinimumVMs"), Integer.class, minVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(minVMsJaxb);
        }
        if (maxVMs >= 0) {
            JAXBElement<Integer> maxVMsJaxb = new JAXBElement<Integer>(new QName("MaximumVMs"), Integer.class, maxVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(maxVMsJaxb);
        }

        return addCloud(c);
    }

    /**
     * Adds the given CloudProvider @cp to the project file.
     *
     * @param cp Cloud provider description object.
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(CloudProviderType cp) throws InvalidElementException {
        validator.validateCloudProvider(cp);

        // If we reach this point means that the SharedDisk is valid
        boolean cloudTagFound = false;
        for (Object obj : this.project.getMasterNodeOrComputeNodeOrDataNode()) {
            if (obj instanceof CloudType) {
                cloudTagFound = true;
                CloudType c = (CloudType) obj;
                JAXBElement<CloudProviderType> cpJaxb =
                    new JAXBElement<CloudProviderType>(new QName("CloudProvider"), CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cpJaxb);
            }
        }

        if (!cloudTagFound) {
            // Create the cloud tag with default values (none)
            CloudType c = new CloudType();
            this.project.getMasterNodeOrComputeNodeOrDataNode().add(c);
            // Add the requested provider
            JAXBElement<CloudProviderType> cpJaxb =
                new JAXBElement<CloudProviderType>(new QName("CloudProvider"), CloudProviderType.class, cp);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cpJaxb);

        }
        return cp;
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images Images description object
     * @param instances Instances description object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, InstanceTypesType instances)
        throws InvalidElementException {
        return addCloudProvider(name, images, instances, -1, null);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images List of Image descriptions
     * @param instances Instances descriptions object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, InstanceTypesType instances)
        throws InvalidElementException {
        ImagesType imagesList = new ImagesType();
        if (images != null) {
            for (ImageType im : images) {
                imagesList.getImage().add(im);
            }
        }

        return addCloudProvider(name, imagesList, instances, -1, null);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images Images description object
     * @param instances List of Instance descriptions
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, List<InstanceTypeType> instances)
        throws InvalidElementException {
        InstanceTypesType instancesList = new InstanceTypesType();
        if (instances != null) {
            for (InstanceTypeType ins : instances) {
                instancesList.getInstanceType().add(ins);
            }
        }

        return addCloudProvider(name, images, instancesList, -1, null);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images List of Image descriptions
     * @param instances List of Instance descriptions
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, List<InstanceTypeType> instances)
        throws InvalidElementException {

        ImagesType imagesList = new ImagesType();
        if (images != null) {
            for (ImageType im : images) {
                imagesList.getImage().add(im);
            }
        }
        InstanceTypesType instancesList = new InstanceTypesType();
        if (instances != null) {
            for (InstanceTypeType ins : instances) {
                instancesList.getInstanceType().add(ins);
            }
        }

        return addCloudProvider(name, imagesList, instancesList, -1, null);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images List of Image descriptions
     * @param instances Instances descriptions object
     * @param limitOfVMs Limit of tasks
     * @param properties provider properties object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, InstanceTypesType instances,
        int limitOfVMs, CloudPropertiesType properties) throws InvalidElementException {

        ImagesType imagesList = new ImagesType();
        if (images != null) {
            for (ImageType im : images) {
                imagesList.getImage().add(im);
            }
        }

        return addCloudProvider(name, imagesList, instances, limitOfVMs, properties);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images Images descriptions object
     * @param instances List of Instance descriptions
     * @param limitOfVMs Limit of tasks
     * @param properties provider properties object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, List<InstanceTypeType> instances,
        int limitOfVMs, CloudPropertiesType properties) throws InvalidElementException {

        InstanceTypesType instancesList = new InstanceTypesType();
        if (instances != null) {
            for (InstanceTypeType ins : instances) {
                instancesList.getInstanceType().add(ins);
            }
        }

        return addCloudProvider(name, images, instancesList, limitOfVMs, properties);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images List of Image descriptions
     * @param instances List of Instance descriptions
     * @param limitOfVMs Limit of tasks
     * @param properties provider properties object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, List<InstanceTypeType> instances,
        int limitOfVMs, CloudPropertiesType properties) throws InvalidElementException {

        ImagesType imagesList = new ImagesType();
        if (images != null) {
            for (ImageType im : images) {
                imagesList.getImage().add(im);
            }
        }
        InstanceTypesType instancesList = new InstanceTypesType();
        if (instances != null) {
            for (InstanceTypeType ins : instances) {
                instancesList.getInstanceType().add(ins);
            }
        }

        return addCloudProvider(name, imagesList, instancesList, limitOfVMs, properties);
    }

    /**
     * Adds a new CloudProvider with the given information.
     *
     * @param name Cloud Provider name
     * @param images Images description object
     * @param instances Instances description object
     * @param limitOfVMs Limit of tasks
     * @param properties provider properties object
     * @return Instance of the added Cloud provider description.
     * @throws InvalidElementException Error validating input data.
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, InstanceTypesType instances,
        int limitOfVMs, CloudPropertiesType properties) throws InvalidElementException {

        CloudProviderType cp = new CloudProviderType();

        // Mandatory elements
        cp.setName(name);
        cp.getImagesOrInstanceTypesOrLimitOfVMs().add(images);
        cp.getImagesOrInstanceTypesOrLimitOfVMs().add(instances);

        // Optional elements
        if (limitOfVMs >= 0) {
            cp.getImagesOrInstanceTypesOrLimitOfVMs().add(limitOfVMs);
        }
        if (properties != null) {
            cp.getImagesOrInstanceTypesOrLimitOfVMs().add(properties);
        }

        return addCloudProvider(cp);
    }

    /* ********* SETTERS: HELPERS FOR SECOND LEVEL ELEMENTS *********/
    /**
     * Sets the general cloud properties to an existing cloud.
     *
     * @param initialVMs Number of initial VMs
     * @param minVMs Minimum number of VMs
     * @param maxVMs Maximum number of VMs
     * @return Returns true if all modifications have been performed, false otherwise
     * @throws InvalidElementException Error validating input data.
     */
    public boolean setCloudProperties(int initialVMs, int minVMs, int maxVMs) throws InvalidElementException {
        CloudType cloud = null;

        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    cloud = (CloudType) obj;
                    break;
                }
            }
        }

        if (cloud == null) {
            // NO cloud tag created, properties cannot be changed
            return false;
        } else {
            if (initialVMs >= 0) {
                JAXBElement<Integer> initialVMsJaxb =
                    new JAXBElement<Integer>(new QName("InitialVMs"), Integer.class, initialVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(initialVMsJaxb);
            }
            if (minVMs >= 0) {
                JAXBElement<Integer> minVMsJaxb =
                    new JAXBElement<Integer>(new QName("MinimumVMs"), Integer.class, minVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(minVMsJaxb);
            }
            if (maxVMs >= 0) {
                JAXBElement<Integer> maxVMsJaxb =
                    new JAXBElement<Integer>(new QName("MaximumVMs"), Integer.class, maxVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(maxVMsJaxb);
            }
        }

        return true;
    }

    /**
     * Adds the given image @image to the cloudProvider with name @cloudProviderName.
     *
     * @cloudProviderName Returns true if image is inserted, false otherwise
     * @param cloudProviderName Cloud Provider Name
     * @param image Image description object
     * @return True if added, false otherwise.
     */
    public boolean addImageToCloudProvider(String cloudProviderName, ImageType image) {
        // Get cloud provider
        CloudProviderType cp = this.getCloudProvider(cloudProviderName);

        // Add image
        if (cp != null) {
            List<Object> objList = cp.getImagesOrInstanceTypesOrLimitOfVMs();
            if (objList != null) {
                for (Object obj : objList) {
                    if (obj instanceof ImagesType) {
                        ImagesType images = (ImagesType) obj;
                        images.getImage().add(image);
                        return true;
                    }
                }
                // No ImagesType tag found, create it and add
                ImagesType images = new ImagesType();
                images.getImage().add(image);
                objList.add(images);
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * Adds the given instance @instance to the cloudProvider @cloudProviderName.
     * 
     * @param cloudProviderName Cloud Provider name
     * @param instance Instance description to add
     * @return True if instance is inserted, false otherwise.
     */
    public boolean addInstanceToCloudProvider(String cloudProviderName, InstanceTypeType instance) {
        // Get cloud provider
        CloudProviderType cp = this.getCloudProvider(cloudProviderName);

        // Add image
        if (cp != null) {
            List<Object> objList = cp.getImagesOrInstanceTypesOrLimitOfVMs();
            if (objList != null) {
                for (Object obj : objList) {
                    if (obj instanceof InstanceTypesType) {
                        InstanceTypesType instances = (InstanceTypesType) obj;
                        instances.getInstanceType().add(instance);
                        return true;
                    }
                }
                // No InstanceTypes tag found, create it and add
                InstanceTypesType instances = new InstanceTypesType();
                instances.getInstanceType().add(instance);
                objList.add(instances);
                return true;
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * Creates an Application description.
     * 
     * @param appDir Application installation directory
     * @param libPath Library path
     * @param cp Class path
     * @param pypath Python path
     * @param environmentScript Path to script to set environment variables
     * @return Created application description object
     */
    public static ApplicationType createApplication(String appDir, String libPath, String cp, String pypath,
        String environmentScript) {
        ApplicationType app = new ApplicationType();
        // Optional parameters
        if (appDir != null && !appDir.isEmpty()) {
            app.setAppDir(appDir);
        }
        if (libPath != null && !libPath.isEmpty()) {
            app.setLibraryPath(libPath);
        }
        if (cp != null && !cp.isEmpty()) {
            app.setClasspath(cp);
        }
        if (pypath != null && !pypath.isEmpty()) {
            app.setPythonpath(pypath);
        }
        if (environmentScript != null && !environmentScript.isEmpty()) {
            app.setEnvironmentScript(environmentScript);
        }
        return app;

    }

    /**
     * Creates a CloudPropertiesType object from a map containing key-value pairs. This method ignores the Context
     * attribute of properties.
     *
     * @param properties Map of key-value properties
     * @return Cloud Properties description object.
     */
    public static CloudPropertiesType createCloudProperties(Map<String, String> properties) {
        CloudPropertiesType cloudProperties = new CloudPropertiesType();
        List<CloudPropertyType> propertyList = cloudProperties.getProperty();

        for (Entry<String, String> e : properties.entrySet()) {
            CloudPropertyType prop = new CloudPropertyType();
            prop.setName(e.getKey());
            prop.setValue(e.getValue());
            propertyList.add(prop);
        }
        return cloudProperties;
    }

    /**
     * Creates a Cloud Image description object.
     * 
     * @param name Image name
     * @param installDir COMPSs Installation directory
     * @param workingDir Working directory
     * @param limitOfTasks Limit of tasks
     * @return created Image description object
     */
    public static ImageType createImage(String name, String installDir, String workingDir, int limitOfTasks) {
        return createImage(name, installDir, workingDir, null, null, null, null, null, null, limitOfTasks, null, null);
    }

    /**
     * Creates a Cloud Image description object.
     * 
     * @param name Image name
     * @param installDir COMPSs Installation directory
     * @param workingDir Working directory
     * @param user Username
     * @param appDir Application installation directory
     * @param libPath Library path
     * @param cp Class path
     * @param pypath Python path
     * @param envScript Path to script to set environment variables
     * @param limitOfTasks Limit of tasks
     * @param pack Package Description
     * @param adaptors Adaptors list description object
     * @return Created Image description object
     */
    public static ImageType createImage(String name, String installDir, String workingDir, String user, String appDir,
        String libPath, String cp, String pypath, String envScript, int limitOfTasks, PackageType pack,
        AdaptorsListType adaptors) {

        ImageType image = new ImageType();

        // Mandatory elements
        image.setName(name);
        JAXBElement<String> installDirJaxb = new JAXBElement<String>(new QName("InstallDir"), String.class, installDir);
        image.getInstallDirOrWorkingDirOrUser().add(installDirJaxb);
        JAXBElement<String> workingDirJaxb = new JAXBElement<String>(new QName("WorkingDir"), String.class, workingDir);
        image.getInstallDirOrWorkingDirOrUser().add(workingDirJaxb);

        // Non mandatory elements
        if (user != null) {
            JAXBElement<String> userJaxb = new JAXBElement<String>(new QName("User"), String.class, user);
            image.getInstallDirOrWorkingDirOrUser().add(userJaxb);
        }
        ApplicationType app = createApplication(appDir, libPath, cp, pypath, envScript);
        JAXBElement<ApplicationType> appsJaxb =
            new JAXBElement<ApplicationType>(new QName("Application"), ApplicationType.class, app);
        image.getInstallDirOrWorkingDirOrUser().add(appsJaxb);
        if (limitOfTasks >= 0) {
            JAXBElement<Integer> limitOfTasksJaxb =
                new JAXBElement<Integer>(new QName("LimitOfTasks"), Integer.class, limitOfTasks);
            image.getInstallDirOrWorkingDirOrUser().add(limitOfTasksJaxb);
        }
        if (pack != null) {
            JAXBElement<PackageType> packJaxb =
                new JAXBElement<PackageType>(new QName("Package"), PackageType.class, pack);
            image.getInstallDirOrWorkingDirOrUser().add(packJaxb);
        }
        if (adaptors != null) {
            JAXBElement<AdaptorsListType> adaptorsJaxb =
                new JAXBElement<AdaptorsListType>(new QName("Adaptors"), AdaptorsListType.class, adaptors);
            image.getInstallDirOrWorkingDirOrUser().add(adaptorsJaxb);
        }
        return image;

    }

    /**
     * Create Cloud Instance type description.
     * 
     * @param name Instance name
     * @return Created Instance type description object.
     */
    public static InstanceTypeType createInstance(String name) {
        InstanceTypeType instance = new InstanceTypeType();
        instance.setName(name);
        return instance;

    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param subsys Submission system description
     * @param nioproperties NIO properties description
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        NIOAdaptorProperties nioproperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<NIOAdaptorProperties> propertiesElement =
            new JAXBElement<NIOAdaptorProperties>(new QName("Ports"), NIOAdaptorProperties.class, nioproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param subsys Submission System description
     * @param gatproperties GAT properties description
     * @param user username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, String gatproperties,
        String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<String> propertiesElement =
            new JAXBElement<String>(new QName("BrokerAdaptor"), String.class, gatproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptors name
     * @param subsys Submission System description
     * @param externalproperties External properties description
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        ExternalAdaptorProperties externalproperties, String user) {

        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<ExternalAdaptorProperties> propertiesElement = new JAXBElement<ExternalAdaptorProperties>(
            new QName("Properties"), ExternalAdaptorProperties.class, externalproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptors name
     * @param subsys Submission System description
     * @param externalProperties List of property descriptions
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        List<PropertyAdaptorType> externalProperties, String user) {

        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        ExternalAdaptorProperties externalpropertiesList = new ExternalAdaptorProperties();
        if (externalProperties != null) {
            for (PropertyAdaptorType pa : externalProperties) {
                externalpropertiesList.getProperty().add(pa);
            }
        }
        JAXBElement<ExternalAdaptorProperties> propertiesElement = new JAXBElement<ExternalAdaptorProperties>(
            new QName("Properties"), ExternalAdaptorProperties.class, externalpropertiesList);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param batch Flag to indicate if submission system is batch
     * @param queues List of queue names
     * @param interactive Flag to indicate if submission system is interactive
     * @param nioproperties NIO properties description
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        NIOAdaptorProperties nioproperties, String user) {

        SubmissionSystemType subsys = new SubmissionSystemType();
        if (batch) {
            BatchType b = new BatchType();
            if (queues != null) {
                for (String q : queues) {
                    b.getQueue().add(q);
                }
            }
            subsys.getBatchOrInteractive().add(b);
        }
        if (interactive) {
            InteractiveType i = new InteractiveType();
            subsys.getBatchOrInteractive().add(i);
        }

        return createAdaptor(name, subsys, nioproperties, user);
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param batch Flag to indicate if submission system is batch
     * @param queues List of queue names
     * @param interactive Flag to indicate if submission system is interactive
     * @param gatproperties GAT properties description
     * @param user Username
     * @return Created Adaptor description object.
     * @return
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        String gatproperties, String user) {

        SubmissionSystemType subsys = new SubmissionSystemType();
        if (batch) {
            BatchType b = new BatchType();
            if (queues != null) {
                for (String q : queues) {
                    b.getQueue().add(q);
                }
            }
            subsys.getBatchOrInteractive().add(b);
        }
        if (interactive) {
            InteractiveType i = new InteractiveType();
            subsys.getBatchOrInteractive().add(i);
        }

        return createAdaptor(name, subsys, gatproperties, user);
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param batch Flag to indicate if submission system is batch
     * @param queues List of queue names
     * @param interactive Flag to indicate if submission system is interactive
     * @param externalProperties External properties description
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        ExternalAdaptorProperties externalProperties, String user) {

        SubmissionSystemType subsys = new SubmissionSystemType();
        if (batch) {
            BatchType b = new BatchType();
            if (queues != null) {
                for (String q : queues) {
                    b.getQueue().add(q);
                }
            }
            subsys.getBatchOrInteractive().add(b);
        }
        if (interactive) {
            InteractiveType i = new InteractiveType();
            subsys.getBatchOrInteractive().add(i);
        }

        return createAdaptor(name, subsys, externalProperties, user);
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param batch Flag to indicate if submission system is batch
     * @param queues List of queue names
     * @param interactive Flag to indicate if submission system is interactive
     * @param externalProperties List of External property descriptions
     * @param user Username
     * @return Created Adaptor description object.
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        List<PropertyAdaptorType> externalProperties, String user) {

        SubmissionSystemType subsys = new SubmissionSystemType();
        if (batch) {
            BatchType b = new BatchType();
            if (queues != null) {
                for (String q : queues) {
                    b.getQueue().add(q);
                }
            }
            JAXBElement<BatchType> batchElement = new JAXBElement<BatchType>(new QName("Batch"), BatchType.class, b);
            subsys.getBatchOrInteractive().add(batchElement);
        }
        if (interactive) {
            InteractiveType i = new InteractiveType();
            JAXBElement<InteractiveType> interactiveElement =
                new JAXBElement<InteractiveType>(new QName("Interactive"), InteractiveType.class, i);
            subsys.getBatchOrInteractive().add(interactiveElement);
        }

        return createAdaptor(name, subsys, externalProperties, user);
    }

    /* ************ DELETERS: MAIN ELEMENTS **************/

    /**
     * Deletes the MasterNode.
     *
     * @return True if deletion is successful, false otherwise.
     */
    public boolean deleteMasterNode() {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof MasterNodeType) {
                    objList.remove(i);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Deletes the ComputeNode with name = @name .
     *
     * @param name Compute node name
     * @return Returns true if deletion is successfull, false otherwise
     */
    public boolean deleteComputeNode(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof ComputeNodeType) {
                    ComputeNodeType cn = (ComputeNodeType) obj;
                    if (cn.getName().equals(name)) {
                        objList.remove(i);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Deletes the DataNode with name = @name .
     *
     * @param name Data node name
     * @return True if deletion is successful, false otherwise.
     */
    public boolean deleteDataNode(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof DataNodeType) {
                    DataNodeType dn = (DataNodeType) obj;
                    if (dn.getName().equals(name)) {
                        objList.remove(i);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Deletes the Service with WSDL = @wsdl.
     *
     * @param wsdl Service WSDL location
     * @return True if deletion is successful, false otherwise.
     */
    public boolean deleteService(String wsdl) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof ServiceType) {
                    ServiceType s = (ServiceType) obj;
                    if (s.getWsdl().equals(wsdl)) {
                        objList.remove(i);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Deletes the Cloud description.
     *
     * @return True if deletion is successful, false otherwise
     */
    public boolean deleteCloud() {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof CloudType) {
                    objList.remove(i);
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Deletes the CloudProvider with name = @name.
     *
     * @param name Cloud provider name
     * @return True if deletion is successful, false otherwise
     */
    public boolean deleteCloudProvider(String name) {
        List<Object> objList = this.project.getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudType) {
                    CloudType cloud = (CloudType) obj;
                    List<JAXBElement<?>> cloudPropsList = cloud.getCloudProviderOrInitialVMsOrMinimumVMs();
                    if (cloudPropsList != null) {
                        for (int i = 0; i < cloudPropsList.size(); ++i) {
                            JAXBElement<?> jaxbelem = cloudPropsList.get(i);
                            if (jaxbelem.getName().equals(new QName("CloudProvider"))) {
                                CloudProviderType cp = (CloudProviderType) jaxbelem.getValue();
                                if (cp.getName().equals(name)) {
                                    cloudPropsList.remove(i);
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    /**
     * Gets a map with the name of the different nodes as the key and the quantity of these cluster nodes as the value
     * of a given ComputingCluster @cluster.
     *
     * @param cluster the cluster
     * @return the map
     */
    public Map<String, Integer> getMapNumberOfCluster(ComputingClusterType cluster) {
        Map<String, Integer> map = new HashMap<>();
        List<ClusterNodeType> listNodes = getListOfClusterNodes(cluster);
        for (ClusterNodeType node : listNodes) {
            map.put(node.getName(), node.getNumberOfNodes());
        }
        return map;
    }

    /**
     * Gets number of clusters.
     *
     * @param cluster the cluster
     * @return the number of clusters
     */
    public int getIntNumberOfCluster(ComputingClusterType cluster) {
        int acc = 0;
        List<ClusterNodeType> listNodes = getListOfClusterNodes(cluster);
        for (ClusterNodeType node : listNodes) {
            acc += node.getNumberOfNodes();
        }
        return acc;
    }

    private List<ClusterNodeType> getListOfClusterNodes(ComputingClusterType c) {
        List<ClusterNodeType> ret = new ArrayList<>();
        List<JAXBElement<?>> elements = c.getLimitOfTasksOrInstallDirOrWorkingDir();
        for (JAXBElement element : elements) {
            if (element.getName().equals(new QName("ClusterNode"))) {
                ret.add((ClusterNodeType) element.getValue());
            }
        }
        return ret;
    }

    /**
     * Gets the software names in a list given a computing cluster, if there is not a software given returns an empty
     * list.
     *
     * @param cluster the cluster
     * @return the list of softwares
     */
    public List<String> getSoftwareNames(ComputingClusterType cluster) {
        List<String> list = new ArrayList<>();
        List<JAXBElement<?>> elements = cluster.getLimitOfTasksOrInstallDirOrWorkingDir();
        for (JAXBElement element : elements) {
            if (element.getName().equals(new QName("Software"))) {
                SoftwareListType software = (SoftwareListType) element.getValue();
                list = software.getApplication();
                break;
            }
        }
        return list;
    }
}
