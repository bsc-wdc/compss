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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.resources.exceptions.InvalidElementException;
import es.bsc.compss.types.resources.exceptions.ResourcesFileValidationException;
import es.bsc.compss.types.resources.jaxb.AdaptorType;
import es.bsc.compss.types.resources.jaxb.AdaptorsListType;
import es.bsc.compss.types.resources.jaxb.AttachedDiskType;
import es.bsc.compss.types.resources.jaxb.AttachedDisksListType;
import es.bsc.compss.types.resources.jaxb.BatchType;
import es.bsc.compss.types.resources.jaxb.CloudProviderType;
import es.bsc.compss.types.resources.jaxb.ClusterNodeType;
import es.bsc.compss.types.resources.jaxb.ComputeNodeType;
import es.bsc.compss.types.resources.jaxb.ComputingClusterType;
import es.bsc.compss.types.resources.jaxb.DataNodeType;
import es.bsc.compss.types.resources.jaxb.EndpointType;
import es.bsc.compss.types.resources.jaxb.HttpType;
import es.bsc.compss.types.resources.jaxb.ImageType;
import es.bsc.compss.types.resources.jaxb.ImagesType;
import es.bsc.compss.types.resources.jaxb.InstanceTypeType;
import es.bsc.compss.types.resources.jaxb.InstanceTypesType;
import es.bsc.compss.types.resources.jaxb.InteractiveType;
import es.bsc.compss.types.resources.jaxb.MemoryType;
import es.bsc.compss.types.resources.jaxb.OSType;
import es.bsc.compss.types.resources.jaxb.OSTypeType;
import es.bsc.compss.types.resources.jaxb.ObjectFactory;
import es.bsc.compss.types.resources.jaxb.PriceType;
import es.bsc.compss.types.resources.jaxb.ProcessorPropertyType;
import es.bsc.compss.types.resources.jaxb.ProcessorType;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesListType;
import es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;
import es.bsc.compss.types.resources.jaxb.ServiceType;
import es.bsc.compss.types.resources.jaxb.SharedDiskType;
import es.bsc.compss.types.resources.jaxb.SoftwareListType;
import es.bsc.compss.types.resources.jaxb.StorageType;
import es.bsc.compss.types.resources.jaxb.SubmissionSystemType;
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

import javax.xml.XMLConstants;
import javax.xml.namespace.QName;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;


public class ResourcesFile {

    // JAXB context
    private JAXBContext context;

    // XSD Schema
    private Schema xsd;

    // Resources instance
    private ResourcesListType resources;

    // Associated validator
    private Validator validator;

    // Logger
    private Logger logger;


    /*
     * ************************************** CONSTRUCTORS
     **************************************/
    /**
     * Creates a ResourceFile instance from a given XML file. The XML is validated against the given schema and
     * internally validated
     *
     * @param xml XML File
     * @param xsd Resources schema
     * @param logger Logger to print debug information
     * @throws JAXBException Error parsing XML
     * @throws ResourcesFileValidationException Error validating data
     */
    public ResourcesFile(File xml, Schema xsd, Logger logger) throws JAXBException, ResourcesFileValidationException {
        this.logger = logger;
        this.logger.info("Init Resources.xml parsing");
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("XML Path: " + xml.getAbsolutePath());
            this.logger.debug("XSD Schema");
        }
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        this.xsd = xsd;

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(xml);
        this.resources = (ResourcesListType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Resources.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Resources.xml finished");
    }

    /**
     * Creates an empty resources file.
     * 
     * @param logger Logger to print debug information
     * @throws JAXBException Error parsing XML
     */
    public ResourcesFile(Logger logger) throws JAXBException {
        this.logger = logger;
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        this.validator = new Validator(this, this.logger);
        this.resources = new ResourcesListType();
    }

    /**
     * Creates a ResourceFile instance from a given XML string. The XML is validated against the given schema and
     * internally validated.
     *
     * @param xmlString XML resources file as string
     * @param xsd Resources XML schema
     * @throws JAXBException Error parsing XML
     * @throws ResourcesFileValidationException Error validating data
     */
    public ResourcesFile(String xmlString, Schema xsd, Logger logger)
        throws JAXBException, ResourcesFileValidationException {
        this.logger = logger;
        this.logger.info("Init Resources.xml parsing");
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("XML String");
            this.logger.debug("XSD Schema");
        }
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        this.xsd = xsd;

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(new StringReader(xmlString));
        this.resources = (ResourcesListType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Resources.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Resources.xml finished");
    }

    /**
     * Creates a ResourceFile instance from a given XML file. The XML is validated against the given path of the schema
     * and internally validated
     *
     * @param xml Resources XML File
     * @param xsdPath Resources schema location path
     * @param logger Logger to print debug information
     * @throws SAXException Error parsing schema
     * @throws JAXBException Error parsing XML
     * @throws ResourcesFileValidationException Error validating data
     */
    public ResourcesFile(File xml, String xsdPath, Logger logger)
        throws SAXException, JAXBException, ResourcesFileValidationException {
        this.logger = logger;
        this.logger.info("Init Resources.xml parsing");
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("XML Path: " + xml.getAbsolutePath());
            this.logger.debug("XSD Path: " + xsdPath);
        }
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(xml);
        this.resources = (ResourcesListType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Resources.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Resources.xml finished");
    }

    /**
     * Creates a ResourceFile instance from a given XML string. The XML is validated against the given path of the
     * schema and internally validated
     *
     * @param xmlString Resources XML as string
     * @param xsdPath Resources XML schema location path
     * @param logger Logger to print debug information
     * @throws SAXException Error parsing XML Schema
     * @throws JAXBException Error parsing XML file
     * @throws ResourcesFileValidationException Error validating data.
     */
    public ResourcesFile(String xmlString, String xsdPath, Logger logger)
        throws SAXException, JAXBException, ResourcesFileValidationException {

        this.logger = logger;
        this.logger.info("Init Resources.xml parsing");
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("XML String");
            this.logger.debug("XSD Path: " + xsdPath);
        }
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));

        // Unmarshall
        Unmarshaller um = this.context.createUnmarshaller();
        um.setSchema(this.xsd);
        JAXBElement<?> jaxbElem = (JAXBElement<?>) um.unmarshal(new StringReader(xmlString));
        this.resources = (ResourcesListType) jaxbElem.getValue();

        // Validate
        this.logger.info("Init Resources.xml validation");
        validator = new Validator(this, this.logger);
        validator.validate();
        this.logger.info("Resources.xml finished");
    }

    /**
     * Create an empty resourceFile object.
     *
     * @param xsdPath Resources XML schema location path
     * @param logger Logger to print debug information
     * @throws SAXException Error parsing Schema
     * @throws JAXBException Error parsing XML data
     * @throws ResourcesFileValidationException Invalid data
     */
    public ResourcesFile(String xsdPath, Logger logger)
        throws SAXException, JAXBException, ResourcesFileValidationException {
        this.logger = logger;
        this.logger.info("Init Resources.xml parsing");
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("XML String");
            this.logger.debug("XSD Path: " + xsdPath);
        }
        this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
        SchemaFactory sf = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        this.xsd = sf.newSchema(new File(xsdPath));
        this.resources = new ResourcesListType();
        validator = new Validator(this, this.logger);
    }

    /* *********** DUMPERS *************/
    /**
     * Stores the current ResourceList object to the given file.
     *
     * @param file File to write resources file
     * @throws JAXBException Error generating XML data
     */
    public void toFile(File file) throws JAXBException {
        logger.info("Resources.xml to file");
        Marshaller m = this.context.createMarshaller();
        ObjectFactory objFact = new ObjectFactory();

        m.setSchema(this.xsd);
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        m.marshal(objFact.createResourcesList(resources), file);
    }

    /**
     * Returns the string construction of the current ResourceList.
     *
     * @return Resources list as string
     * @throws JAXBException Error generating XML data.
     */
    public String getString() throws JAXBException {
        logger.info("Resources.xml to string");
        Marshaller m = this.context.createMarshaller();
        ObjectFactory objFact = new ObjectFactory();

        m.setSchema(this.xsd);
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);

        StringWriter sw = new StringWriter();
        m.marshal(objFact.createResourcesList(resources), sw);
        return sw.getBuffer().toString();
    }

    /*
     * ************ GETTERS: MAIN ELEMENTS LISTS
     *****************/
    /**
     * Returns the JAXB class representing the all XML file content.
     *
     * @return Resources list object
     */
    public ResourcesListType getResources() {
        return this.resources;
    }

    /**
     * Returns a list of declared Shared Disks.
     *
     * @return List of Shared disk objects
     */
    public List<SharedDiskType> getSharedDisks_list() {
        ArrayList<SharedDiskType> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof SharedDiskType) {
                    list.add((SharedDiskType) obj);
                }
            }
        }

        return list;
    }

    /**
     * Returns a list of declared DataNodes.
     *
     * @return List of Data node objects
     */
    public List<DataNodeType> getDataNodes_list() {
        ArrayList<DataNodeType> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a list of declared ComputeNodes.
     *
     * @return List of Compute node objects
     */
    public List<ComputeNodeType> getComputeNodes_list() {
        ArrayList<ComputeNodeType> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a list of declared Services.
     *
     * @return List of Service objects
     */
    public List<ServiceType> getServices_list() {
        ArrayList<ServiceType> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a list of declared CloudProviders.
     *
     * @return List of Cloud Providers
     */
    public List<CloudProviderType> getCloudProviders_list() {
        ArrayList<CloudProviderType> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudProviderType) {
                    list.add((CloudProviderType) obj);
                }
            }
        }

        return list;
    }

    /* ************* GETTERS: MAIN ELEMENTS HASH-MAPS ***************/

    /**
     * Returns a HashMap of declared Shared Disks (Key: Name, Value: SD).
     *
     * @return
     */
    public HashMap<String, SharedDiskType> getSharedDisks_hashMap() {
        HashMap<String, SharedDiskType> res = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof SharedDiskType) {
                    String diskName = ((SharedDiskType) obj).getName();
                    res.put(diskName, (SharedDiskType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Returns a HashMap of declared DataNodes (Key: Name, Value: DN).
     *
     * @return
     */
    public HashMap<String, DataNodeType> getDataNodes_hashMap() {
        HashMap<String, DataNodeType> res = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof DataNodeType) {
                    String dataNodeName = ((DataNodeType) obj).getName();
                    res.put(dataNodeName, (DataNodeType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Returns a HashMap of declared ComputeNodes (Key: Name, Value: CN).
     *
     * @return
     */
    public HashMap<String, ComputeNodeType> getComputeNodes_hashMap() {
        HashMap<String, ComputeNodeType> res = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    String computeNodeName = ((ComputeNodeType) obj).getName();
                    res.put(computeNodeName, (ComputeNodeType) obj);
                }
            }
        }

        return res;
    }

    /**
     * Gets the mount points defined in the different compute nodes.
     *
     * @param diskName Name of the disk
     * @return Map of Compute Node name: Mount point
     */
    public HashMap<String, String> getDiskMountPointsInComputeNodes(String diskName) {
        HashMap<String, String> mountPoints = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputeNodeType) {
                    ComputeNodeType cn = (ComputeNodeType) obj;
                    HashMap<String, String> disks = getSharedDisks(cn);
                    if (disks != null && disks.containsKey(diskName)) {
                        mountPoints.put(cn.getName(), disks.get(diskName));
                    }
                }
            }
        }

        return mountPoints;
    }

    /**
     * Returns a HashMap of declared Services (Key: WSDL, Value: Service).
     *
     * @return
     */
    public HashMap<String, ServiceType> getServices_hashMap() {
        HashMap<String, ServiceType> res = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a HashMap of declared Shared Disks (Key: Name, Value: List of Services).
     *
     * @return
     */
    public HashMap<String, List<ServiceType>> getServices_byName() {
        HashMap<String, List<ServiceType>> res = new HashMap<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    String serviceName = ((ServiceType) obj).getName();

                    List<ServiceType> auxServicesList = null;
                    if (res.containsKey(serviceName)) {
                        auxServicesList = res.get(serviceName);
                        auxServicesList.add((ServiceType) obj);
                    } else {
                        auxServicesList = new ArrayList<>();
                    }
                    res.put(serviceName, auxServicesList);
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
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudProviderType) {
                    String cloudProviderName = ((CloudProviderType) obj).getName();
                    res.put(cloudProviderName, (CloudProviderType) obj);
                }
            }
        }

        return res;
    }

    /* ********** GETTERS: MAIN ELEMENTS KEY-VALUES (NAME) *************/

    /**
     * Returns a List of the names of the declared SharedDisks.
     *
     * @return
     */
    public List<String> getSharedDisks_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof SharedDiskType) {
                    list.add(((SharedDiskType) obj).getName());
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
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a List of the names of the declared ComputeNodes.
     *
     * @return
     */
    public List<String> getComputeNodes_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a List of the names of the declared ComputeNodes.
     *
     * @return
     */
    public List<String> getComputingCluster_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputingClusterType) {
                    list.add(((ComputingClusterType) obj).getName());
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the WSDLs of the declared Services.
     *
     * @return
     */
    public List<String> getServices_wsdls() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns a List of the WSDLs of the declared Services.
     *
     * @return
     */
    public List<String> getHttp_urls() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof HttpType) {
                    list.add(((HttpType) obj).getBaseUrl());
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the names of the declared Services (without repetition).
     *
     * @return
     */
    public List<String> getServices_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ServiceType) {
                    String serviceName = ((ServiceType) obj).getName();
                    if (!list.contains(serviceName)) {
                        list.add(((ServiceType) obj).getName());
                    }
                }
            }
        }

        return list;
    }

    /**
     * Returns a List of the names of the declared CloudProviders.
     *
     * @return
     */
    public List<String> getCloudProviders_names() {
        ArrayList<String> list = new ArrayList<>();
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudProviderType) {
                    list.add(((CloudProviderType) obj).getName());
                }
            }
        }

        return list;
    }

    /* *********** GETTERS: MAIN ELEMENTS SINGLE **************/

    /**
     * Returns the SharedDisk with name=@name.
     *
     * @param name Shared disk name
     * @return Null if name doesn't exist
     */
    public SharedDiskType getSharedDisk(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof SharedDiskType) {
                    SharedDiskType sd = (SharedDiskType) obj;
                    if (sd.getName().equals(name)) {
                        return sd;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the DataNode with name=@name. Null if name doesn't exist
     *
     * @param name Data Node name
     * @return Null if name doesn't exist
     */
    public DataNodeType getDataNode(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns the Host of a given DataNode.
     *
     * @param d Data node object
     * @return
     */
    public String getHost(DataNodeType d) {
        List<JAXBElement<?>> elements = d.getHostOrPathOrAdaptors();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("Host"))) {
                    return (String) elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the path of a given DataNode.
     *
     * @param d Data node object
     * @return
     */
    public String getPath(DataNodeType d) {
        List<JAXBElement<?>> elements = d.getHostOrPathOrAdaptors();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("Path"))) {
                    return (String) elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the storage size of a given DataNode.
     *
     * @param d Data node object
     * @return
     */
    public float getStorageSize(DataNodeType d) {
        List<JAXBElement<?>> elements = d.getHostOrPathOrAdaptors();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("Storage"))) {
                    StorageType storage = ((StorageType) elem.getValue());
                    return getStorageSize(storage);
                }
            }
        }

        return (float) -1.0;
    }

    /**
     * Returns the storage size of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return storage size
     */
    public float getStorageSize(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageSize(storage);
                }
            }
        }

        return (float) -1.0;
    }

    /**
     * Returns the storage size of a given InstanceType.
     *
     * @param instance Instance type description
     * @return
     */
    public float getStorageSize(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageSize(storage);
                }
            }
        }

        return (float) -1.0;
    }

    /**
     * Returns the storage size of a given InstanceType.
     *
     * @param cluster Cluster Node type description
     * @return
     */
    public float getStorageSize(ClusterNodeType cluster) {
        StorageType storage = getStorage(cluster);
        if (storage != null) {
            return getStorageSize(storage);
        }
        return (float) -1.0;
    }

    /**
     * Get storage size.
     * 
     * @param storage Storage description
     * @return
     */
    private float getStorageSize(StorageType storage) {
        List<Serializable> storageProps = storage.getSizeOrTypeOrBandwidth();
        if (storageProps != null) {
            for (Serializable prop : storageProps) {
                if (prop instanceof Float) {
                    return (float) prop;
                }
            }
        }
        return (float) -1.0;
    }

    private StorageType getStorage(ClusterNodeType c) {
        List<Object> objList = c.getMaxNumNodesOrProcessorOrOperatingSystem();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    return ((StorageType) obj);
                }
            }
        }
        return null;
    }

    /**
     * Returns the storage type of a given DataNode.
     *
     * @param d Data node object
     * @return
     */
    public String getStorageType(DataNodeType d) {
        List<JAXBElement<?>> elements = d.getHostOrPathOrAdaptors();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("Storage"))) {
                    StorageType storage = ((StorageType) elem.getValue());
                    return getStorageType(storage);
                }
            }
        }

        return null;
    }

    /**
     * Returns the storage type of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return Storage type
     */
    public String getStorageType(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageType(storage);
                }
            }
        }

        return null;
    }

    /**
     * Returns the storage type of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return Storage type
     */
    public String getStorageType(ClusterNodeType c) {
        StorageType storage = getStorage(c);
        if (storage != null) {
            return getStorageType(storage);
        }
        return null;
    }

    /**
     * Returns the storage type of a given InstanceType.
     *
     * @param instance Instance type description
     * @return
     */
    public String getStorageType(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageType(storage);
                }
            }
        }

        return null;
    }

    /**
     * Get storage type.
     * 
     * @param storage Storage description
     * @return
     */
    private String getStorageType(StorageType storage) {
        List<Serializable> storageProps = storage.getSizeOrTypeOrBandwidth();
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
     * Returns the storage bandwidth of a given DataNode.
     *
     * @param d Data node object
     * @return
     */
    public int getStorageBW(DataNodeType d) {
        List<JAXBElement<?>> elements = d.getHostOrPathOrAdaptors();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("Storage"))) {
                    StorageType storage = ((StorageType) elem.getValue());
                    return getStorageBW(storage);
                }
            }
        }

        return -1;
    }

    /**
     * Returns the storage bandwidth of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return storage size
     */
    public int getStorageBW(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageBW(storage);
                }
            }
        }

        return -1;
    }

    /**
     * Returns the storage bandwidth of a given ClusterNode.
     *
     * @param c Cluster node description object
     * @return storage size
     */
    public int getStorageBW(ClusterNodeType c) {
        StorageType storage = getStorage(c);
        if (storage != null) {
            return getStorageBW(storage);
        }
        return -1;
    }

    /**
     * Returns the storage bandwidth of a given InstanceType.
     *
     * @param instance Instance type description
     * @return
     */
    public int getStorageBW(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof StorageType) {
                    StorageType storage = ((StorageType) obj);
                    return getStorageBW(storage);
                }
            }
        }

        return -1;
    }

    /**
     * Get storage bandwidth.
     * 
     * @param storage Storage description
     * @return
     */
    private int getStorageBW(StorageType storage) {
        List<Serializable> storageProps = storage.getSizeOrTypeOrBandwidth();
        if (storageProps != null) {
            for (Serializable prop : storageProps) {
                if (prop instanceof Integer) {
                    return (int) prop;
                }
            }
        }
        return -1;
    }

    /**
     * Returns the ComputeNode with name=@name. Null if name doesn't exist
     *
     * @param name Compute node name
     * @return
     */
    public ComputeNodeType getComputeNode(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns the ComputingCluster with name=@name. Null if name doesn't exist
     *
     * @param name Compute Cluster name
     * @return
     */
    public ComputingClusterType getComputingCluster(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ComputingClusterType) {
                    ComputingClusterType cl = (ComputingClusterType) obj;
                    if (cl.getName().equals(name)) {
                        return cl;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the ClusterNode with name=@name inside the given cluster @cluster.
     *
     * @param cluster the cluster
     * @param name the name of the node
     * @return the cluster node
     */
    public ClusterNodeType getClusterNode(ComputingClusterType cluster, String name) {
        List<ClusterNodeType> list = cluster.getClusterNode();
        if (list == null) {
            return null;
        }
        for (ClusterNodeType node : list) {
            if (node.getName().equals(name)) {
                return node;
            }
        }
        // Not found
        return null;
    }

    /**
     * Gets a list of a ll processors in a map with the key as the cluster node name.
     *
     * @param c the c
     * @return the map processors
     */
    public Map<String, List<ProcessorType>> getMapProcessors(ComputingClusterType c) {
        Map<String, List<ProcessorType>> map = new HashMap<>();
        List<ClusterNodeType> objList = c.getClusterNode();
        if (objList != null) {
            for (ClusterNodeType node : objList) {
                String clusterName = node.getName();
                map.put(clusterName, getProcessors(node));
            }
        }
        return map;
    }

    /**
     * Returns the processors of a given Compute Node @c.
     *
     * @param c Compute node object
     * @return List of Processor description objects
     */
    public List<ProcessorType> getProcessors(ComputeNodeType c) {
        List<ProcessorType> processors = new ArrayList<>();

        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ProcessorType) {
                    processors.add(((ProcessorType) obj));
                }
            }
        }

        return processors;
    }

    /**
     * Returns the all processors of a given ComputingClusterType @c.
     *
     * @param c Computing Cluster
     * @return List of Processor description objects
     */
    public List<ProcessorType> getProcessors(ComputingClusterType c) {
        List<ClusterNodeType> objList = c.getClusterNode();
        List<ProcessorType> processors = new ArrayList<>();
        if (objList != null) {
            for (ClusterNodeType node : objList) {
                processors.addAll(getProcessors(node));
            }
        }
        return processors;
    }

    private List<ProcessorType> getProcessors(ClusterNodeType node) {
        List<ProcessorType> processors = new ArrayList<>();
        List<Object> objList = node.getMaxNumNodesOrProcessorOrOperatingSystem();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ProcessorType) {
                    processors.add(((ProcessorType) obj));
                }
            }
        }
        return processors;
    }

    /**
     * Returns the processors of a given InstanceType.
     *
     * @param instance Instance description
     * @return
     */
    public List<ProcessorType> getProcessors(InstanceTypeType instance) {
        List<ProcessorType> processors = new ArrayList<>();

        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof ProcessorType) {
                    processors.add(((ProcessorType) obj));
                }
            }
        }

        return processors;
    }

    /**
     * Returns the memory size of a given ComputeNode.
     *
     * @param c Compute node object
     * @return memory size
     */
    public float getMemorySize(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MemoryType) {
                    MemoryType mem = ((MemoryType) obj);
                    List<Serializable> memProps = mem.getSizeOrType();
                    if (memProps != null) {
                        for (Serializable prop : memProps) {
                            if (prop instanceof Float) {
                                return (float) prop;
                            }
                        }
                    } else {
                        return (float) -1.0;
                    }
                }
            }
        }

        return (float) -1.0;
    }

    /**
     * Returns the memory size of a given InstanceType.
     *
     * @param instance Instance description
     * @return
     */
    public float getMemorySize(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MemoryType) {
                    MemoryType mem = ((MemoryType) obj);
                    List<Serializable> memProps = mem.getSizeOrType();
                    if (memProps != null) {
                        for (Serializable prop : memProps) {
                            if (prop instanceof Float) {
                                return (float) prop;
                            }
                        }
                    } else {
                        return (float) -1.0;
                    }
                }
            }
        }

        return (float) -1.0;
    }

    /**
     * Returns the memory size of a given ClusterNodeType.
     *
     * @param cluster Cluster node description
     * @return
     */
    public float getMemorySize(ClusterNodeType cluster) {
        MemoryType mem = this.getMemory(cluster);
        if (mem != null) {
            List<Serializable> memProps = mem.getSizeOrType();
            if (memProps != null) {
                for (Serializable prop : memProps) {
                    if (prop instanceof Float) {
                        return (float) prop;
                    }
                }
            }
        }
        return (float) -1.0;
    }

    private MemoryType getMemory(ClusterNodeType cluster) {
        List<Object> objList = cluster.getMaxNumNodesOrProcessorOrOperatingSystem();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MemoryType) {
                    return (MemoryType) obj;
                }
            }
        }
        return null;
    }

    /**
     * Returns the memory type of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return memory type
     */
    public String getMemoryType(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MemoryType) {
                    MemoryType mem = ((MemoryType) obj);
                    List<Serializable> memProps = mem.getSizeOrType();
                    if (memProps != null) {
                        for (Serializable prop : memProps) {
                            if (prop instanceof String) {
                                return (String) prop;
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
     * Returns the memory type of a given ClusterNode.
     *
     * @param c Cluster Node description object
     * @return memory type
     */
    public String getMemoryType(ClusterNodeType c) {
        MemoryType mem = getMemory(c);
        if (mem != null) {
            List<Serializable> memProps = mem.getSizeOrType();
            if (memProps != null) {
                for (Serializable prop : memProps) {
                    if (prop instanceof String) {
                        return (String) prop;
                    }
                }
            }
        }
        return null;
    }

    /**
     * Returns the memory type of a given InstanceType.
     *
     * @param instance Instance type description
     * @return
     */
    public String getMemoryType(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof MemoryType) {
                    MemoryType mem = ((MemoryType) obj);
                    List<Serializable> memProps = mem.getSizeOrType();
                    if (memProps != null) {
                        for (Serializable prop : memProps) {
                            if (prop instanceof String) {
                                return (String) prop;
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
     * Returns the OperatingSystem properties of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public OSType getOperatingSystem(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        return getOperatingSystem(objList);
    }

    private OSType getOperatingSystem(List<Object> objList) {
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof OSType) {
                    return ((OSType) obj);
                }
            }
        }
        return null;
    }

    /**
     * Returns the OperatingSystem properties of a given Cluster ComputeNode.
     *
     * @param c Cluster Compute node description object
     * @return
     */
    public OSType getOperatingSystem(ClusterNodeType c) {
        List<Object> objList = c.getMaxNumNodesOrProcessorOrOperatingSystem();
        return getOperatingSystem(objList);
    }

    /**
     * Returns the OperatingSystem type of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public String getOperatingSystemType(ComputeNodeType c) {
        OSType os = getOperatingSystem(c);
        if (os != null) {
            return getOperatingSystemType(os);
        }

        return null;
    }

    /**
     * Returns the OS Type associated to a given Image.
     *
     * @param image Image description
     * @return
     */
    public String getOperatingSystemType(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        OSType os = getOperatingSystem(objList);
        if (os != null) {
            return getOperatingSystemType(os);
        }
        return null;
    }

    /**
     * Returns the OS type.
     *
     * @param os Operating System description
     * @return
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
     * Returns the OperatingSystem distribution of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public String getOperatingSystemDistribution(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        OSType os = getOperatingSystem(objList);
        if (os != null) {
            return getOperatingSystemDistribution(os);
        }
        return null;
    }

    /**
     * Returns the OS Distribution associated to a given Image.
     *
     * @param image Image description
     * @return
     */
    public String getOperatingSystemDistribution(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        OSType os = getOperatingSystem(objList);
        if (os != null) {
            return getOperatingSystemDistribution(os);
        }
        return null;
    }

    /**
     * Returns the OS Distribution.
     *
     * @param os Operating System description
     * @return
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
     * Returns the OperatingSystem Version of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public String getOperatingSystemVersion(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        OSType os = getOperatingSystem(objList);
        if (os != null) {
            return getOperatingSystemVersion(os);
        }
        return null;
    }

    /**
     * Returns the OperatingSystem Version of a given Computing Cluster node.
     *
     * @param c Computing cluster node description object
     * @return
     */
    public String getOperatingSystemVersion(ClusterNodeType c) {
        OSType os = getOperatingSystem(c);
        if (os != null) {
            return getOperatingSystemVersion(os);
        }
        return null;
    }

    /**
     * Returns the OS Version associated to a given Image.
     *
     * @param image Image description
     * @return
     */
    public String getOperatingSystemVersion(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        OSType os = getOperatingSystem(objList);
        if (os != null) {
            return getOperatingSystemVersion(os);
        }
        return null;
    }

    /**
     * Returns the OS Version.
     *
     * @param os Operating System description
     * @return
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
     * Returns the applications of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public List<String> getApplications(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        return getApplications(objList);
    }

    /**
     * Returns the applications of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public List<String> getApplications(ClusterNodeType c) {
        List<Object> objList = c.getMaxNumNodesOrProcessorOrOperatingSystem();
        return getApplications(objList);
    }

    private List<String> getApplications(List<Object> objList) {
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof SoftwareListType) {
                    return ((SoftwareListType) obj).getApplication();
                }
            }
        }
        return null;
    }

    /**
     * Returns the applications associated to a given image.
     *
     * @param image Image description
     * @return
     */
    public List<String> getApplications(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        return getApplications(objList);
    }

    /**
     * Returns the price properties of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public PriceType getPrice(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof PriceType) {
                    return ((PriceType) obj);
                }
            }
        }

        return null;
    }

    /**
     * Returns the price information associated to a given image.
     *
     * @param image Image description
     * @return
     */
    public PriceType getPrice(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof PriceType) {
                    return ((PriceType) obj);
                }
            }
        }

        return null;
    }

    /**
     * Returns the price information of a given InstanceType.
     *
     * @param instance Instance type description
     * @return
     */
    public PriceType getPrice(InstanceTypeType instance) {
        List<Object> objList = instance.getProcessorOrMemoryOrStorage();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof PriceType) {
                    return ((PriceType) obj);
                }
            }
        }

        return null;
    }

    /**
     * Returns the AttacSharedDisks of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    private static AttachedDisksListType getAttachedSharedDisks(ComputeNodeType c) {
        List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof AttachedDisksListType) {
                    return (AttachedDisksListType) obj;
                }
            }
        }

        return null;
    }

    /**
     * Returns the SharedDisks (name, mountpoint) of a given ComputeNode.
     *
     * @param c Compute node description object
     * @return
     */
    public HashMap<String, String> getSharedDisks(ComputeNodeType c) {

        AttachedDisksListType disks = getAttachedSharedDisks(c);
        if (disks != null) {
            HashMap<String, String> disksInformation = new HashMap<>();
            for (AttachedDiskType disk : disks.getAttachedDisk()) {
                disksInformation.put(disk.getName(), disk.getMountPoint());
            }
            return disksInformation;
        }

        return null;

    }

    /**
     * Returns the SharedDisks (name, mountpoint) of a given Computing Cluster.
     *
     * @param c Compute cluster description object
     * @return
     */
    public HashMap<String, String> getSharedDisks(ComputingClusterType c) {
        HashMap<String, String> disksInformation = new HashMap<>();
        AttachedDisksListType disks = c.getSharedDisks();
        if (disks != null) {
            for (AttachedDiskType disk : disks.getAttachedDisk()) {
                disksInformation.put(disk.getName(), disk.getMountPoint());
            }
        }
        if (disksInformation.isEmpty()) {
            return null;
        } else {
            return disksInformation;
        }
    }

    /**
     * Returns the shared Disks associated to a given image.
     *
     * @param image Image description
     * @return
     */
    public HashMap<String, String> getSharedDisks(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof AttachedDisksListType) {
                    List<AttachedDiskType> disks = ((AttachedDisksListType) obj).getAttachedDisk();
                    if (disks != null) {
                        HashMap<String, String> result = new HashMap<String, String>();
                        for (AttachedDiskType disk : disks) {
                            result.put(disk.getName(), disk.getMountPoint());
                        }
                        return result;
                    } else {
                        return null;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Returns the queues of a given Adaptor within a given ComputeNode.
     *
     * @param cn Compute node description object
     * @param adaptorName Adaptor name
     * @return
     */
    public List<String> getAdaptorQueues(ComputeNodeType cn, String adaptorName) {
        List<String> adatorQueues = new ArrayList<>();

        List<Object> objList = cn.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof AdaptorsListType) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) obj).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return getAdaptorQueues(adaptor);
                            }
                        }
                    } else {
                        return adatorQueues; // Empty
                    }
                }
            }
        }

        return adatorQueues; // Empty
    }

    /**
     * Returns the queues of a given Adaptor within a given Image.
     *
     * @param image Image description
     * @param adaptorName Adaptor name
     * @return
     */
    public List<String> getAdaptorQueues(ImageType image, String adaptorName) {
        List<String> adaptorQueues = new ArrayList<>();
        AdaptorType adaptor = getAdaptor(image, adaptorName);
        if (adaptor != null) {
            return getAdaptorQueues(adaptor);
        }
        return adaptorQueues; // Empty
    }

    /**
     * Returns the adaptor queues.
     *
     * @param adaptor Adaptor description
     * @return
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
     * Returns the declared properties of a given Adaptor within a given ComputeNode.
     *
     * @param cn Compute node description object
     * @param adaptorName Adaptor Name
     * @return
     */
    public AdaptorType getAdaptor(ComputeNodeType cn, String adaptorName) {
        List<Object> objList = cn.getProcessorOrAdaptorsOrMemory();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof AdaptorsListType) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) obj).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return adaptor;
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
     * Returns the declared properties of a given Adaptor within a given ImageType.
     *
     * @param im Image description object
     * @param adaptorName Adaptor Name
     * @return
     */
    public AdaptorType getAdaptor(ImageType im, String adaptorName) {
        List<Object> objList = im.getAdaptorsOrOperatingSystemOrSoftware();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof AdaptorsListType) {
                    List<AdaptorType> adaptors = ((AdaptorsListType) obj).getAdaptor();
                    if (adaptors != null) {
                        // Loop for specific adaptor name
                        for (AdaptorType adaptor : adaptors) {
                            if (adaptor.getName().equals(adaptorName)) {
                                return adaptor;
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
     * Returns the declared properties of a given Adaptor within a given ComputeNode.
     *
     * @param cn Compute node description object
     * @param adaptorName Adaptor name
     * @return
     */
    public Map<String, Object> getAdaptorProperties(ComputeNodeType cn, String adaptorName) {

        AdaptorType adaptor = getAdaptor(cn, adaptorName);
        if (adaptor != null) {
            return getAdaptorProperties(adaptor);
        }

        return null;
    }

    /**
     * Returns the declared properties of a given Adaptor within a given ComputingCluster.
     *
     * @param cluster the cluster
     * @param adaptorName the adaptor name
     * @return the adaptor properties
     */
    public Map<String, Object> getAdaptorProperties(ComputingClusterType cluster, String adaptorName) {
        if (cluster.getAdaptors() == null) {
            return null;
        }
        for (AdaptorType adaptor : cluster.getAdaptors().getAdaptor()) {
            if (adaptor.getName().equals(adaptorName)) {
                return getAdaptorProperties(adaptor);
            }
        }

        return null;
    }

    /**
     * Returns the declared properties of a given Adaptor within a given Image.
     *
     * @param image Image description
     * @param adaptorName Adaptor name
     * @return
     */
    public Map<String, Object> getAdaptorProperties(ImageType image, String adaptorName) {
        AdaptorType adaptor = getAdaptor(image, adaptorName);
        if (adaptor != null) {
            return getAdaptorProperties(adaptor);
        }
        return null;
    }

    /**
     * Returns the adaptor properties.
     *
     * @param adaptor Adaptor description
     * @return
     */
    public Map<String, Object> getAdaptorProperties(AdaptorType adaptor) {
        HashMap<String, Object> properties = new HashMap<String, Object>();
        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            // Loop for submission system
            for (JAXBElement<?> adaptorElement : innerElements) {
                if (adaptorElement.getName().equals(new QName("Ports"))
                    || adaptorElement.getName().equals(new QName("BrokerAdaptor"))
                    || adaptorElement.getName().equals(new QName("Properties"))) {

                    properties.put(adaptorElement.getName().getLocalPart(), (Object) adaptorElement.getValue());
                }
            }
        }

        return properties;
    }

    /**
     * Returns the Service with wsdl=@wsdl. Null if name doesn't exist
     *
     * @param wsdl Service WSDL
     * @return
     */
    public ServiceType getService(String wsdl) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Returns the HTTP Service with BaseUrl=@baseUrl. Null if name doesn't exist
     *
     * @param baseUrl Http service base url
     * @return
     */
    public HttpType getHttpService(String baseUrl) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof HttpType) {
                    HttpType s = (HttpType) obj;
                    if (s.getBaseUrl().equals(baseUrl)) {
                        return s;
                    }
                }
            }
        }
        // Not found
        return null;
    }

    /**
     * Returns the CloudProvider with name=@name. Null if name doesn't exist
     *
     * @param name Cloud Provider name
     * @return
     */
    public CloudProviderType getCloudProvider(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (Object obj : objList) {
                if (obj instanceof CloudProviderType) {
                    CloudProviderType cp = (CloudProviderType) obj;
                    if (cp.getName().equals(name)) {
                        return cp;
                    }
                }
            }
        }

        // Not found
        return null;
    }

    /**
     * Returns the instance with the given name within the given CloudProvider. Null if not found
     * 
     * @param cp Cloud provider description
     * @param instanceName Cloud Instance name
     * @return
     */
    public InstanceTypeType getInstance(CloudProviderType cp, String instanceName) {
        InstanceTypesType instances = cp.getInstanceTypes();
        if (instances != null) {
            for (InstanceTypeType instance : instances.getInstanceType()) {
                if (instance.getName().equals(instanceName)) {
                    return instance;
                }
            }
        }

        return null;
    }

    /**
     * Returns the image with the given name within the given CloudProvider. Null if not found
     *
     * @param cp Cloud provider description
     * @param imageName Cloud Image name
     * @return
     */
    public ImageType getImage(CloudProviderType cp, String imageName) {
        ImagesType images = cp.getImages();
        if (images != null) {
            for (ImageType image : images.getImage()) {
                if (image.getName().equals(imageName)) {
                    return image;
                }
            }
        }

        return null;
    }

    /**
     * Returns the connector main class information form a given endpoint.
     *
     * @param endpoint Connector endpoint
     * @return
     */
    public String getConnectorMainClass(EndpointType endpoint) {
        List<JAXBElement<String>> elements = endpoint.getServerOrConnectorJarOrConnectorClass();
        if (elements != null) {
            for (JAXBElement<String> elem : elements) {
                if (elem.getName().equals(new QName("ConnectorClass"))) {
                    return elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the connector jar file information form a given endpoint.
     *
     * @param endpoint Connector endpoint
     * @return
     */
    public String getConnectorJarPath(EndpointType endpoint) {
        List<JAXBElement<String>> elements = endpoint.getServerOrConnectorJarOrConnectorClass();
        if (elements != null) {
            for (JAXBElement<String> elem : elements) {
                if (elem.getName().equals(new QName("ConnectorJar"))) {
                    return elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the server information form a given endpoint.
     *
     * @param endpoint Connector endpoint
     * @return
     */
    public String getServer(EndpointType endpoint) {
        List<JAXBElement<String>> elements = endpoint.getServerOrConnectorJarOrConnectorClass();
        if (elements != null) {
            for (JAXBElement<String> elem : elements) {
                if (elem.getName().equals(new QName("Server"))) {
                    return elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the port information form a given endpoint.
     *
     * @param endpoint Connector endpoint
     * @return
     */
    public String getPort(EndpointType endpoint) {
        List<JAXBElement<String>> elements = endpoint.getServerOrConnectorJarOrConnectorClass();
        if (elements != null) {
            for (JAXBElement<String> elem : elements) {
                if (elem.getName().equals(new QName("Port"))) {
                    return elem.getValue();
                }
            }
        }

        return null;
    }

    /**
     * Returns the image creation time associated to a given image.
     *
     * @param image Image description
     * @return
     */
    public int getCreationTime(ImageType image) {
        List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
        if (objList != null) {
            // Loop for adaptors tag
            for (Object obj : objList) {
                if (obj instanceof Integer) { // Creation time
                    return ((Integer) obj);
                }
            }
        }

        return -1;
    }

    /* ****************** ADDERS: MAIN ELEMENTS ********************** */

    /**
     * Adds the given SharedDisk @sd to the resources file.
     * 
     * @param sd Shared disk description
     * @return return the added Shared disk
     * @throws InvalidElementException Error invalid data.
     */
    public SharedDiskType addSharedDisk(SharedDiskType sd) throws InvalidElementException {
        validator.validateSharedDisk(sd);

        // If we reach this point means that the SharedDisk is valid
        this.resources.getSharedDiskOrDataNodeOrComputeNode().add(sd);
        return sd;
    }

    /**
     * Adds a new SharedDisk with name=@name and returns the instance of the new SharedDisk.
     *
     * @param name Shared disk name
     * @return Added shared disk description
     * @throws InvalidElementException Error invalid data
     */
    public SharedDiskType addSharedDisk(String name) throws InvalidElementException {
        SharedDiskType sd = new SharedDiskType();
        sd.setName(name);
        sd.setStorage(null);

        return this.addSharedDisk(sd);
    }

    /**
     * Adds a new SharedDisk with name=@name and storage=@storage and returns the instance of the new SharedDisk.
     *
     * @param name Shared disk name
     * @param storage Storage description
     * @return Added shared disk description
     * @throws InvalidElementException Error invalid data
     */
    public SharedDiskType addSharedDisk(String name, StorageType storage) throws InvalidElementException {
        SharedDiskType sd = new SharedDiskType();
        sd.setName(name);
        sd.setStorage(storage);

        return this.addSharedDisk(sd);
    }

    /**
     * Adds the given DataNode @dn to the resources file.
     *
     * @param dn Data node description
     * @return Added Data node description
     * @throws InvalidElementException Error validating data
     */
    public DataNodeType addDataNode(DataNodeType dn) throws InvalidElementException {
        validator.validateDataNode(dn);

        // If we reach this point means that the DataNode is valid
        this.resources.getSharedDiskOrDataNodeOrComputeNode().add(dn);
        return dn;
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node adaptors list object
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, AdaptorsListType adaptors)
        throws InvalidElementException {
        return this.addDataNode(name, host, path, adaptors, null, (AttachedDisksListType) null);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node List of adaptor objects
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, List<AdaptorType> adaptors)
        throws InvalidElementException {
        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return this.addDataNode(name, host, path, adaptorsList, null, (AttachedDisksListType) null);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node List of adaptor objects
     * @param storage Storage description
     * @param sharedDisks Shared Disk list object
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, List<AdaptorType> adaptors,
        StorageType storage, AttachedDisksListType sharedDisks) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return this.addDataNode(name, host, path, adaptorsList, storage, sharedDisks);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node adaptors list object
     * @param storage Storage description
     * @param sharedDisks List of shared disk object
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, AdaptorsListType adaptors,
        StorageType storage, List<AttachedDiskType> sharedDisks) throws InvalidElementException {

        AttachedDisksListType sharedDisksList = new AttachedDisksListType();
        if (sharedDisks != null) {
            for (AttachedDiskType d : sharedDisks) {
                sharedDisksList.getAttachedDisk().add(d);
            }
        }

        return this.addDataNode(name, host, path, adaptors, storage, sharedDisksList);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node List of adaptor objects
     * @param storage Storage description
     * @param sharedDisks List of shared disk objects
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, List<AdaptorType> adaptors,
        StorageType storage, List<AttachedDiskType> sharedDisks) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        AttachedDisksListType sharedDisksList = new AttachedDisksListType();
        if (sharedDisks != null) {
            for (AttachedDiskType d : sharedDisks) {
                sharedDisksList.getAttachedDisk().add(d);
            }
        }

        return this.addDataNode(name, host, path, adaptorsList, storage, sharedDisksList);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode.
     *
     * @param name Data node name
     * @param host Data node host
     * @param path Data node path
     * @param adaptors Data node adaptors list object
     * @param storage Storage description
     * @param sharedDisks Shared Disk list object
     * @return Added Data node description
     * @throws InvalidElementException Error invalid data
     */
    public DataNodeType addDataNode(String name, String host, String path, AdaptorsListType adaptors,
        StorageType storage, AttachedDisksListType sharedDisks) throws InvalidElementException {
        DataNodeType dn = new DataNodeType();
        dn.setName(name);

        JAXBElement<String> hostElement = new JAXBElement<String>(new QName("Host"), String.class, host);
        dn.getHostOrPathOrAdaptors().add(hostElement);

        JAXBElement<String> pathElement = new JAXBElement<String>(new QName("Path"), String.class, path);
        dn.getHostOrPathOrAdaptors().add(pathElement);

        JAXBElement<AdaptorsListType> adaptorsElement =
            new JAXBElement<AdaptorsListType>(new QName("Adaptors"), AdaptorsListType.class, adaptors);
        dn.getHostOrPathOrAdaptors().add(adaptorsElement);

        // Optional parameters
        if (storage != null) {
            JAXBElement<StorageType> storageElement =
                new JAXBElement<StorageType>(new QName("Storage"), StorageType.class, storage);
            dn.getHostOrPathOrAdaptors().add(storageElement);
        }
        if (sharedDisks != null) {
            JAXBElement<AttachedDisksListType> sdsElement = new JAXBElement<AttachedDisksListType>(
                new QName("SharedDisks"), AttachedDisksListType.class, sharedDisks);
            dn.getHostOrPathOrAdaptors().add(sdsElement);
        }

        return this.addDataNode(dn);
    }

    /**
     * Adds the given ComputeNode @cn to the resources file.
     *
     * @param cn Compute node description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(ComputeNodeType cn) throws InvalidElementException {
        validator.validateComputeNode(cn);

        // If we reach this point means that the ComputeNode is valid
        this.resources.getSharedDiskOrDataNodeOrComputeNode().add(cn);
        return cn;
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors Adaptors list type
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors)
        throws InvalidElementException {
        return this.addComputeNode(name, processors, adaptors, null, null, null, (SoftwareListType) null,
            (AttachedDisksListType) null, null);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors List of adaptors
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, List<AdaptorType> adaptors)
        throws InvalidElementException {
        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return this.addComputeNode(name, processors, adaptorsList, null, null, null, (SoftwareListType) null,
            (AttachedDisksListType) null, null);
    }

    /**
     * Add a compute node with a single instances of processor and adaptor.
     *
     * @param name Compute node name
     * @param processor Processors description
     * @param adaptor Adaptor description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, ProcessorType processor, AdaptorType adaptor)
        throws InvalidElementException {
        AdaptorsListType adaptorsList = new AdaptorsListType();
        adaptorsList.getAdaptor().add(adaptor);
        List<ProcessorType> processors = new ArrayList<ProcessorType>();
        processors.add(processor);

        return this.addComputeNode(name, processors, adaptorsList, null, null, null, (SoftwareListType) null,
            (AttachedDisksListType) null, null);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors Adaptors list object
     * @param memory Memory description
     * @param storage Storage description
     * @param os OS description
     * @param applications List of applications
     * @param sharedDisks Attached disk list object
     * @param price Price description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors,
        MemoryType memory, StorageType storage, OSType os, List<String> applications, AttachedDisksListType sharedDisks,
        PriceType price) throws InvalidElementException {

        SoftwareListType software = new SoftwareListType();
        if (applications != null) {
            for (String app : applications) {
                software.getApplication().add(app);
            }
        }

        return this.addComputeNode(name, processors, adaptors, memory, storage, os, software, sharedDisks, price);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors Adaptors list object
     * @param memory Memory description
     * @param storage Storage description
     * @param os OS description
     * @param software Software list object
     * @param sharedDisks Attached disk list object
     * @param price Price description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors,
        MemoryType memory, StorageType storage, OSType os, SoftwareListType software,
        List<AttachedDiskType> sharedDisks, PriceType price) throws InvalidElementException {

        AttachedDisksListType sharedDisksList = new AttachedDisksListType();
        if (sharedDisks != null) {
            for (AttachedDiskType d : sharedDisks) {
                sharedDisksList.getAttachedDisk().add(d);
            }
        }

        return this.addComputeNode(name, processors, adaptors, memory, storage, os, software, sharedDisksList, price);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors List of adaptors
     * @param memory Memory description
     * @param storage Storage description
     * @param os OS description
     * @param applications List of applications
     * @param sharedDisks list attached shared disks
     * @param price Price description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, List<AdaptorType> adaptors,
        MemoryType memory, StorageType storage, OSType os, List<String> applications,
        List<AttachedDiskType> sharedDisks, PriceType price) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        SoftwareListType software = null;
        if (applications != null) {
            software = new SoftwareListType();
            for (String app : applications) {
                software.getApplication().add(app);
            }
        }
        AttachedDisksListType sharedDisksList = null;
        if (sharedDisks != null) {
            sharedDisksList = new AttachedDisksListType();
            for (AttachedDiskType d : sharedDisks) {
                sharedDisksList.getAttachedDisk().add(d);
            }
        }

        return this.addComputeNode(name, processors, adaptorsList, memory, storage, os, software, sharedDisksList,
            price);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors List of adaptors
     * @param memory Memory description
     * @param storage Storage description
     * @param os OS description
     * @param software List of software
     * @param sharedDisks attached shared disks list object
     * @param price Price description
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors,
        MemoryType memory, StorageType storage, OSType os, SoftwareListType software, AttachedDisksListType sharedDisks,
        PriceType price) throws InvalidElementException {

        ComputeNodeType cn = new ComputeNodeType();

        cn.setName(name);
        if (processors != null) {
            for (ProcessorType p : processors) {
                cn.getProcessorOrAdaptorsOrMemory().add(p);
            }
        }
        cn.getProcessorOrAdaptorsOrMemory().add(adaptors);

        // Optional parameters
        if (memory != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(memory);
        }
        if (storage != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(storage);
        }
        if (os != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(os);
        }
        if (software != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(software);
        }
        if (sharedDisks != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(sharedDisks);
        }
        if (price != null) {
            cn.getProcessorOrAdaptorsOrMemory().add(price);
        }

        return this.addComputeNode(cn);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode.
     *
     * @param name Compute node name
     * @param processors List of processors
     * @param adaptors List of adaptors
     * @param memorySize Memory size
     * @param diskSize Disk size
     * @param osName OS name
     * @return Added Computed node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, List<AdaptorType> adaptors,
        float memorySize, float diskSize, String osName) throws InvalidElementException {

        ComputeNodeType cn = new ComputeNodeType();

        cn.setName(name);
        if (processors != null) {
            for (ProcessorType p : processors) {
                cn.getProcessorOrAdaptorsOrMemory().add(p);
            }
        }
        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        cn.getProcessorOrAdaptorsOrMemory().add(adaptorsList);

        // Optional parameters
        if (memorySize > 0) {
            MemoryType memory = createMemory(memorySize, null);
            cn.getProcessorOrAdaptorsOrMemory().add(memory);
        }
        if (diskSize > 0) {
            StorageType storage = createStorage(diskSize, null, -1);
            cn.getProcessorOrAdaptorsOrMemory().add(storage);
        }
        if (osName != null) {
            if (!osName.isEmpty()) {
                OSType os = createOperatingSystem(osName, null, null);
                cn.getProcessorOrAdaptorsOrMemory().add(os);
            }
        }

        return this.addComputeNode(cn);
    }

    /**
     * Add an instance of compute node with a single processor and a NIO adaptor.
     *
     * @param name Node name
     * @param procName Processor Name
     * @param procCU Processor Computing Units
     * @param procArch Processor Architecture
     * @param procSpeed Processor Speed
     * @param procType Processor Type
     * @param procMemSize Processor Internal Memory Size
     * @param procProp Processor Property
     * @param adaptorName Adaptor Name
     * @param maxPort Maximum port number of the port range
     * @param minPort Minimum port number of the port range
     * @param executor Executor command
     * @param user Username
     * @param memorySize Memory size
     * @param memoryType Memory type
     * @param storageSize Storage size
     * @param storageType Storage type
     * @param storageBW Storage Bandwidth
     * @param osType Operating system type
     * @param osDistribution Operating system distribution
     * @param osVersion Operating system version
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, String procName, int procCU, String procArch, float procSpeed,
        String procType, float procMemSize, ProcessorPropertyType procProp, String adaptorName, int maxPort,
        int minPort, String executor, String user, float memorySize, String memoryType, float storageSize,
        String storageType, int storageBW, String osType, String osDistribution, String osVersion)
        throws InvalidElementException {

        List<ProcessorType> processors = new ArrayList<>();
        ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
        processors.add(pr);
        ResourcesNIOAdaptorProperties nioProp = new ResourcesNIOAdaptorProperties();
        nioProp.setMaxPort(maxPort);
        nioProp.setMinPort(minPort);
        nioProp.setRemoteExecutionCommand(executor);
        List<AdaptorType> adaptors = new ArrayList<>();
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, user);
        adaptors.add(adaptor);
        MemoryType mem = createMemory(memorySize, memoryType);
        StorageType storage = createStorage(storageSize, storageType, storageBW);
        OSType os = createOperatingSystem(osType, osDistribution, osVersion);
        return this.addComputeNode(name, processors, adaptors, mem, storage, os, null, null, null);
    }

    /**
     * Add an instance of compute node with a single processor and a NIO adaptor.
     * 
     * @param name Compute node name
     * @param procName Processor name
     * @param procCU Computing units
     * @param adaptorName Adaptor name
     * @param maxPort adaptor max port
     * @param minPort adaptor min port
     * @return Added compute node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, String procName, int procCU, String adaptorName, int maxPort,
        int minPort) throws InvalidElementException {
        List<ProcessorType> processors = new ArrayList<>();
        ProcessorType pr = createProcessor(procName, procCU);
        processors.add(pr);
        List<AdaptorType> adaptors = new ArrayList<>();
        ResourcesNIOAdaptorProperties nioProp = new ResourcesNIOAdaptorProperties();
        nioProp.setMaxPort(maxPort);
        nioProp.setMinPort(minPort);
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, null);
        adaptors.add(adaptor);
        return this.addComputeNode(name, processors, adaptors, null, null, null, null, null, null);
    }

    /**
     * Add an instance of compute node with a single processor and a GAT adaptor.
     *
     * @param name Node name
     * @param procName Processor Name
     * @param procCU Processor Computing Units
     * @param procArch Processor Architecture
     * @param procSpeed Processor Speed
     * @param procProp Processor Property
     * @param adaptorName Adaptor Name
     * @param batch Flag to indicate is adaptor submission system supports batch
     * @param queues List of available queues
     * @param interactive Flag to indicate is adaptor submission system supports interactive
     * @param brokerAdaptor GAT broker adaptor
     * @param user User name
     * @param memorySize Memory size
     * @param memoryType Memory type
     * @param storageSize Storage size
     * @param storageType Storage type
     * @param storageBW Storage Bandwidth
     * @param osType Operating system type
     * @param osDistribution Operating system distribution
     * @param osVersion Operating system version
     * @return Added computing node
     * @throws InvalidElementException Error invalid data
     */
    public ComputeNodeType addComputeNode(String name, String procName, int procCU, String procArch, float procSpeed,
        String procType, float procMemSize, ProcessorPropertyType procProp, String adaptorName, boolean batch,
        List<String> queues, boolean interactive, String brokerAdaptor, String user, float memorySize,
        String memoryType, float storageSize, String storageType, int storageBW, String osType, String osDistribution,
        String osVersion) throws InvalidElementException {

        List<ProcessorType> processors = new ArrayList<>();
        ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
        processors.add(pr);
        MemoryType mem = createMemory(memorySize, memoryType);
        StorageType storage = createStorage(storageSize, storageType, storageBW);
        OSType os = createOperatingSystem(osType, osDistribution, osVersion);
        List<AdaptorType> adaptors = new ArrayList<>();
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, batch, queues, interactive, brokerAdaptor, user);
        adaptors.add(adaptor);
        return this.addComputeNode(name, processors, adaptors, mem, storage, os, null, null, null);
    }

    /**
     * Add the given Service @s to the resources file.
     *
     * @param s Service description
     * @return Added service
     * @throws InvalidElementException Error invalid data
     */
    public ServiceType addService(ServiceType s) throws InvalidElementException {
        validator.validateService(s);

        // If we reach this point means that the DataNode is valid
        this.resources.getSharedDiskOrDataNodeOrComputeNode().add(s);
        return s;
    }

    /**
     * Adds a new Service with the given information and returns the instance of the new Service.
     *
     * @param wsdl WSDL location
     * @param name Service name
     * @param namespace Service namespace
     * @param port Service port
     * @return Added service description
     * @throws InvalidElementException Error invalid data
     */
    public ServiceType addService(String wsdl, String name, String namespace, String port)
        throws InvalidElementException {
        return this.addService(wsdl, name, namespace, port, null);
    }

    /**
     * Adds a new Service with the given information and returns the instance of the new Service.
     *
     * @param wsdl WSDL location
     * @param name Service name
     * @param namespace Service namespace
     * @param port Service port
     * @param price Price description
     * @return Added service description
     * @throws InvalidElementException Error invalid data
     */
    public ServiceType addService(String wsdl, String name, String namespace, String port, PriceType price)
        throws InvalidElementException {
        ServiceType s = new ServiceType();
        s.setWsdl(wsdl);
        s.setName(name);
        s.setNamespace(namespace);
        s.setPort(port);

        // Optional parameters
        if (price != null) {
            s.setPrice(price);
        }

        return this.addService(s);
    }

    /**
     * Adds the given CloudProvider @cp to the resources file.
     *
     * @param cp Cloud provider
     * @return Added cloud provider
     * @throws InvalidElementException Error invalid data
     */
    public CloudProviderType addCloudProvider(CloudProviderType cp) throws InvalidElementException {
        validator.validateCloudProvider(cp);

        // If we reach this point means that the DataNode is valid
        this.resources.getSharedDiskOrDataNodeOrComputeNode().add(cp);
        return cp;
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider.
     *
     * @param name Cloud provider name
     * @param endpoint Cloud provider endpoint
     * @param images Cloud images list
     * @param instances Cloud instance list
     * @return Added Cloud provider
     * @throws InvalidElementException Error invalid data
     */
    public CloudProviderType addCloudProvider(String name, EndpointType endpoint, ImagesType images,
        InstanceTypesType instances) throws InvalidElementException {
        CloudProviderType cp = new CloudProviderType();

        cp.setName(name);
        cp.setEndpoint(endpoint);
        cp.setImages(images);
        cp.setInstanceTypes(instances);

        return this.addCloudProvider(cp);
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider.
     *
     * @param name Cloud provider name
     * @param endpoint Cloud provider endpoint
     * @param images Cloud images list
     * @param instances Cloud instance list
     * @return Added Cloud provider
     * @throws InvalidElementException Error invalid data
     */
    public CloudProviderType addCloudProvider(String name, EndpointType endpoint, List<ImageType> images,
        List<InstanceTypeType> instances) throws InvalidElementException {

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

        return this.addCloudProvider(name, endpoint, imagesList, instancesList);
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider.
     *
     * @param name Cloud provider name
     * @param server Cloud provider server
     * @param connectorJar Cloud provider connector JAR
     * @param connectorClass Cloud provider connector class
     * @param images Cloud images list
     * @param instances Cloud instance list
     * @return Added Cloud provider
     * @throws InvalidElementException Error invalid data
     */
    public CloudProviderType addCloudProvider(String name, String server, String connectorJar, String connectorClass,
        List<ImageType> images, List<InstanceTypeType> instances) throws InvalidElementException {

        EndpointType endpoint = new EndpointType();
        JAXBElement<String> serverElement = new JAXBElement<String>(new QName("Server"), String.class, server);
        endpoint.getServerOrConnectorJarOrConnectorClass().add(serverElement);
        JAXBElement<String> connectorClassElement =
            new JAXBElement<String>(new QName("ConnectorClass"), String.class, connectorClass);
        JAXBElement<String> connectorJarElement =
            new JAXBElement<String>(new QName("ConnectorJar"), String.class, connectorJar);
        endpoint.getServerOrConnectorJarOrConnectorClass().add(connectorClassElement);
        endpoint.getServerOrConnectorJarOrConnectorClass().add(connectorJarElement);

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

        return this.addCloudProvider(name, endpoint, imagesList, instancesList);
    }

    /* ************ SETTERS: HELPERS FOR SECOND LEVEL ELEMENTS ********************/
    /**
     * Creates a Processor element.
     *
     * @param name Processor Name
     * @param cu Processor Computing Units
     * @param procArchitecture Processor Architecture
     * @param procSpeed Processor Speed
     * @param type Processor Type
     * @param internalMemory processor internal memory
     * @param procProperties Processor Property
     * @return Created processor element
     */
    public static ProcessorType createProcessor(String name, int cu, String procArchitecture, float procSpeed,
        String type, float internalMemory, ProcessorPropertyType procProperties) {

        ProcessorType processor = new ProcessorType();
        processor.setName(name);
        JAXBElement<Integer> cuElement = new JAXBElement<Integer>(new QName("ComputingUnits"), Integer.class, cu);
        processor.getComputingUnitsOrArchitectureOrSpeed().add(cuElement);

        if (procArchitecture != null) {
            JAXBElement<String> archElement =
                new JAXBElement<String>(new QName("Architecture"), String.class, procArchitecture);
            processor.getComputingUnitsOrArchitectureOrSpeed().add(archElement);
        }

        if (procSpeed > 0) {
            JAXBElement<Float> speedElement = new JAXBElement<Float>(new QName("Speed"), Float.class, procSpeed);
            processor.getComputingUnitsOrArchitectureOrSpeed().add(speedElement);
        }
        if (type != null) {
            JAXBElement<String> typeElement = new JAXBElement<String>(new QName("Type"), String.class, type);
            processor.getComputingUnitsOrArchitectureOrSpeed().add(typeElement);
        } else {
            JAXBElement<String> typeElement = new JAXBElement<String>(new QName("Type"), String.class, "CPU");
            processor.getComputingUnitsOrArchitectureOrSpeed().add(typeElement);
        }
        if (internalMemory > 0) {
            JAXBElement<Float> memElement =
                new JAXBElement<Float>(new QName("InternalMemorySize"), Float.class, internalMemory);
            processor.getComputingUnitsOrArchitectureOrSpeed().add(memElement);
        }
        if (procProperties != null) {
            JAXBElement<ProcessorPropertyType> propElement = new JAXBElement<ProcessorPropertyType>(
                new QName("ProcessorProperty"), ProcessorPropertyType.class, procProperties);
            processor.getComputingUnitsOrArchitectureOrSpeed().add(propElement);
        }
        return processor;
    }

    public static ProcessorType createProcessor(String name, int cu) {
        return createProcessor(name, cu, null, 0, null, 0, null);
    }

    /**
     * Creates a Processor Property element.
     *
     * @param key Processor property key
     * @param value Processor property value
     * @return Created processor
     */
    public static ProcessorPropertyType createProcessorProperty(String key, String value) {
        ProcessorPropertyType prop = new ProcessorPropertyType();
        prop.setKey(key);
        prop.setValue(value);
        return prop;
    }

    /**
     * Creates a Memory element.
     *
     * @param memorySize Memory size
     * @param type Memory type
     * @return created memory
     */
    public static MemoryType createMemory(float memorySize, String type) {
        MemoryType mem = new MemoryType();
        mem.getSizeOrType().add(new Float(memorySize));
        if (type != null) {
            mem.getSizeOrType().add(type);
        }
        return mem;
    }

    /**
     * Creates a Storage element.
     *
     * @param storageSize Storage size
     * @param type Storage type
     * @param storageBW storage bandwidth
     * @return created storage
     */
    public static StorageType createStorage(float storageSize, String type, int storageBW) {
        StorageType storage = new StorageType();
        storage.getSizeOrTypeOrBandwidth().add(new Float(storageSize));
        if (type != null) {
            storage.getSizeOrTypeOrBandwidth().add(type);
        }
        if (storageBW != -1) {
            storage.getSizeOrTypeOrBandwidth().add(storageBW);
        }
        return storage;
    }

    /**
     * Creates a Operating System element.
     *
     * @param osType Operating System type (linux, windows, ...)
     * @param osDistribution Operating System Distribution (Ubuntu, Centos, ...)
     * @param osVersion Operating System version
     * @return
     */
    public static OSType createOperatingSystem(String osType, String osDistribution, String osVersion) {
        OSType os = new OSType();
        OSTypeType osTypeType = OSTypeType.fromValue(osType);
        JAXBElement<OSTypeType> typeElement =
            new JAXBElement<OSTypeType>(new QName("Type"), OSTypeType.class, osTypeType);
        os.getTypeOrDistributionOrVersion().add(typeElement);

        // Optional parameters
        if (osDistribution != null) {
            JAXBElement<String> distElement =
                new JAXBElement<String>(new QName("Distribution"), String.class, osDistribution);
            os.getTypeOrDistributionOrVersion().add(distElement);
        }
        if (osVersion != null) {
            JAXBElement<String> verElement = new JAXBElement<String>(new QName("Version"), String.class, osVersion);
            os.getTypeOrDistributionOrVersion().add(verElement);
        }

        return os;
    }

    /**
     * Creates a Cloud image description.
     * 
     * @param name Image name
     * @param adaptorName Adaptor name
     * @param batch Batch flag
     * @param queues List of queues
     * @param interactive Interactive flag
     * @param brokerAdaptor Broker adaptor
     * @param user username
     * @param osType OS type
     * @param osDistribution OS distribution
     * @param osVersion OS vesion
     * @return Created Cloud Image
     */
    public static ImageType createImage(String name, String adaptorName, boolean batch, List<String> queues,
        boolean interactive, String brokerAdaptor, String user, String osType, String osDistribution,
        String osVersion) {

        ImageType image = new ImageType();
        image.setName(name);
        AdaptorsListType adaptorsList = new AdaptorsListType();
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, batch, queues, interactive, brokerAdaptor, user);
        adaptorsList.getAdaptor().add(adaptor);
        image.getAdaptorsOrOperatingSystemOrSoftware().add(adaptorsList);
        OSType os = createOperatingSystem(osType, osDistribution, osVersion);
        image.getAdaptorsOrOperatingSystemOrSoftware().add(os);
        return image;

    }

    /**
     * Creates a Cloud image description.
     * 
     * @param name Image name
     * @param adaptorName Adaptor name
     * @param maxPort Maximum port number
     * @param minPort Minimum port number
     * @param executor Executor
     * @param user username
     * @param osType OS type
     * @param osDistribution OS distribution
     * @param osVersion OS vesion
     * @return Created Cloud Image
     */
    public static ImageType createImage(String name, String adaptorName, int maxPort, int minPort, String executor,
        String user, String osType, String osDistribution, String osVersion) {

        ImageType image = new ImageType();
        image.setName(name);
        ResourcesNIOAdaptorProperties nioProp = new ResourcesNIOAdaptorProperties();
        nioProp.setMaxPort(maxPort);
        nioProp.setMinPort(minPort);
        nioProp.setRemoteExecutionCommand(executor);
        AdaptorsListType adaptorsList = new AdaptorsListType();
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, user);
        adaptorsList.getAdaptor().add(adaptor);
        image.getAdaptorsOrOperatingSystemOrSoftware().add(adaptorsList);
        OSType os = createOperatingSystem(osType, osDistribution, osVersion);
        image.getAdaptorsOrOperatingSystemOrSoftware().add(os);
        return image;

    }

    /**
     * Creates a Cloud image description.
     * 
     * @param name Image name
     * @param adaptorName Adaptor name
     * @param maxPort Maximum port number
     * @param minPort Minimum port number
     * @return Created Cloud Image
     */
    public static ImageType createImage(String name, String adaptorName, int maxPort, int minPort) {
        ImageType image = new ImageType();
        image.setName(name);
        AdaptorsListType adaptorsList = new AdaptorsListType();
        ResourcesNIOAdaptorProperties nioProp = new ResourcesNIOAdaptorProperties();
        nioProp.setMaxPort(maxPort);
        nioProp.setMinPort(minPort);
        AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, null);
        adaptorsList.getAdaptor().add(adaptor);
        image.getAdaptorsOrOperatingSystemOrSoftware().add(adaptorsList);
        return image;
    }

    public static InstanceTypeType createInstance(String name, String procName, int procCU, float memorySize,
        float storageSize) {
        return createInstance(name, procName, procCU, null, 0, null, 0, null, memorySize, null, storageSize, null, -1);
    }

    /**
     * Create cloud instance type.
     * 
     * @param name Instance type name
     * @param procName Processor name
     * @param procCU Processor computing units
     * @param procArch Processor architecture
     * @param procSpeed Processor Speed
     * @param procType Processor type
     * @param procMemSize Processor internal memory size
     * @param procProp Processor properties
     * @param memorySize Memory size
     * @param memoryType Memory type
     * @param storageSize Storage size
     * @param storageType Storage type
     * @param storageBW Storage Bandwidth
     * @return Created instance type description
     */
    public static InstanceTypeType createInstance(String name, String procName, int procCU, String procArch,
        float procSpeed, String procType, float procMemSize, ProcessorPropertyType procProp, float memorySize,
        String memoryType, float storageSize, String storageType, int storageBW) {

        InstanceTypeType instance = new InstanceTypeType();
        instance.setName(name);
        ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
        instance.getProcessorOrMemoryOrStorage().add(pr);
        MemoryType mem = createMemory(memorySize, memoryType);
        instance.getProcessorOrMemoryOrStorage().add(mem);
        StorageType storage = createStorage(storageSize, storageType, storageBW);
        instance.getProcessorOrMemoryOrStorage().add(storage);
        return instance;
    }

    /**
     * Create cloud endpoint.
     * 
     * @param server Cloud server
     * @param connectorClass Connector class
     * @param connectorJar Connector Jar
     * @param port Port
     * @return Created cloud server endpoint
     */
    public static EndpointType createEndpoint(String server, String connectorClass, String connectorJar, String port) {
        EndpointType endPoint = new EndpointType();

        JAXBElement<String> serverElement = new JAXBElement<String>(new QName("Server"), String.class, server);
        endPoint.getServerOrConnectorJarOrConnectorClass().add(serverElement);

        JAXBElement<String> connectorClassElement =
            new JAXBElement<String>(new QName("ConnectorClass"), String.class, connectorClass);
        endPoint.getServerOrConnectorJarOrConnectorClass().add(connectorClassElement);

        JAXBElement<String> connectorJarElement =
            new JAXBElement<String>(new QName("ConnectorJar"), String.class, connectorJar);
        endPoint.getServerOrConnectorJarOrConnectorClass().add(connectorJarElement);

        if (port != null) {
            JAXBElement<String> portElement = new JAXBElement<String>(new QName("Port"), String.class, port);
            endPoint.getServerOrConnectorJarOrConnectorClass().add(portElement);
        }
        return endPoint;
    }

    /**
     * Creates an instance of an Adaptor with the given information.
     *
     * @param name Adaptor name
     * @param subsys Submission System
     * @param nioproperties NIO properties
     * @param user username
     * @return Created adaptor
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        ResourcesNIOAdaptorProperties nioproperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<ResourcesNIOAdaptorProperties> propertiesElement =
            new JAXBElement<>(new QName("Ports"), ResourcesNIOAdaptorProperties.class, nioproperties);
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
     * @param subsys Submission System
     * @param gatproperties GAT properties
     * @param user Username
     * @return Created adaptor
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
     * @param name Adaptor name
     * @param subsys Submission System
     * @param externalproperties External properties
     * @param user username
     * @return Created adaptor
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        ResourcesExternalAdaptorProperties externalproperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<ResourcesExternalAdaptorProperties> propertiesElement =
            new JAXBElement<>(new QName("Properties"), ResourcesExternalAdaptorProperties.class, externalproperties);
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
     * @param subsys Submission system
     * @param externalProperties External properties
     * @param user Username
     * @return Created Adaptor
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
        List<ResourcesPropertyAdaptorType> externalProperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement =
            new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        ResourcesExternalAdaptorProperties externalpropertiesList = new ResourcesExternalAdaptorProperties();
        if (externalProperties != null) {
            for (ResourcesPropertyAdaptorType pa : externalProperties) {
                externalpropertiesList.getProperty().add(pa);
            }
        }
        JAXBElement<ResourcesExternalAdaptorProperties> propertiesElement = new JAXBElement<>(new QName("Properties"),
            ResourcesExternalAdaptorProperties.class, externalpropertiesList);
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
     * @param name Adaptor's name
     * @param batch Flag to indicate if supports batch submission
     * @param queues Batch queues
     * @param interactive Flag to indicate if supports interactive submission
     * @param nioproperties NIO adaptor properties
     * @param user Username
     * @return
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        ResourcesNIOAdaptorProperties nioproperties, String user) {

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
     * @param name Adaptor's name
     * @param batch Flag to indicate if supports batch submission
     * @param queues Batch queues
     * @param interactive Flag to indicate if supports interactive submission
     * @param gatproperties GAT broker adaptor
     * @param user Username
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
     * @param batch Batch flag
     * @param queues LIst of queues
     * @param interactive Interactive flag
     * @param externalProperties External properties
     * @param user username
     * @return
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        ResourcesExternalAdaptorProperties externalProperties, String user) {

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
     * @param batch Batch flag
     * @param queues List of queues
     * @param interactive Interactive flag
     * @param externalProperties External properties
     * @param user Username
     * @return
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive,
        List<ResourcesPropertyAdaptorType> externalProperties, String user) {

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
     * Adds the given image @image to the cloudProvider with name = @cloudProviderName.
     * 
     * @param cloudProviderName Cloud provider name
     * @param image Image description
     * @return True if image is inserted, false otherwise
     */
    public boolean addImageToCloudProvider(String cloudProviderName, ImageType image) {
        // Get cloud provider
        CloudProviderType cp = this.getCloudProvider(cloudProviderName);

        // Add image
        if (cp != null) {
            cp.getImages().getImage().add(image);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adds the given instance @instance to the cloudProvider with name = @cloudProviderName.
     * 
     * @param cloudProviderName Cloud Provider name
     * @param instance Instance Type description
     * @return True if instance is inserted, false otherwise
     */
    public boolean addInstanceToCloudProvider(String cloudProviderName, InstanceTypeType instance) {
        // Get cloud provider
        CloudProviderType cp = this.getCloudProvider(cloudProviderName);

        // Add image
        if (cp != null) {
            cp.getInstanceTypes().getInstanceType().add(instance);
            return true;
        } else {
            return false;
        }
    }

    /**
     * Attach shared disk to a compute node.
     * 
     * @param diskName Shared Disk name
     * @param cnName Compute node name
     * @param mountPoint Mount point path
     * @throws InvalidElementException Error validating data
     */
    public void attachSharedDiskToComputeNode(String diskName, String cnName, String mountPoint)
        throws InvalidElementException {
        ComputeNodeType cn = getComputeNode(cnName);
        if (cn != null) {

            AttachedDisksListType disks = getAttachedSharedDisks(cn);
            if (disks == null) {
                disks = new AttachedDisksListType();
                cn.getProcessorOrAdaptorsOrMemory().add(disks);
            }
            AttachedDiskType d = new AttachedDiskType();
            d.setName(diskName);
            d.setMountPoint(mountPoint);
            disks.getAttachedDisk().add(d);
        } else {
            throw new InvalidElementException("ComputeNodeType", cnName, "Not found");
        }
    }

    /**
     * Detached Shared disk to compute node.
     * 
     * @param diskName Shared disk name
     * @param cnName Compute node name
     * @throws InvalidElementException Error invalid data
     */
    public void detachSharedDiskToComputeNode(String diskName, String cnName) throws InvalidElementException {
        ComputeNodeType cn = getComputeNode(cnName);
        if (cn != null) {

            AttachedDisksListType disks = getAttachedSharedDisks(cn);
            if (disks != null) {
                for (int i = 0; i < disks.getAttachedDisk().size(); i++) {
                    if (disks.getAttachedDisk().get(i).getName().equals(diskName)) {
                        disks.getAttachedDisk().remove(i);
                        return;
                    }
                }
                throw new InvalidElementException("AttachedDisksType", diskName, "Not found");
            } else {
                throw new InvalidElementException("AttachedDisksListType", "none", "Not found");
            }

        } else {
            throw new InvalidElementException("ComputeNodeType", cnName, "Not found");
        }
    }

    /* ******************* GETTERS: HELPERS FOR SECOND LEVEL ELEMENTS *********************/

    /**
     * Returns the number of Compunting Units of a given processor @p.
     *
     * @param p Processor
     * @return
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
     * @param p Processor
     * @return
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
     * @param p Processor
     * @return
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
     * @param p Processor
     * @return
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
     * @param p Processor
     * @return
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
     * @param p Processor
     * @return
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

    /* ************* DELETERS: MAIN ELEMENTS ***************/

    /**
     * Deletes the SharedDisk with name=@name Returns true if deletion is successful, false otherwise.
     *
     * @param name Shared Disk name
     * @return
     */
    public boolean deleteSharedDisk(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof SharedDiskType) {
                    SharedDiskType sd = (SharedDiskType) obj;
                    if (sd.getName().equals(name)) {
                        objList.remove(i);
                        return true;
                    }
                }
            }
        }

        return false;
    }

    /**
     * Deletes the DataNode with name=@name Returns true if deletion is successful, false otherwise.
     *
     * @param name Data node name
     * @return
     */
    public boolean deleteDataNode(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Deletes the ComputeNode with name=@name Returns true if deletion is successfull, false otherwise.
     *
     * @param name Compute node name
     * @return
     */
    public boolean deleteComputeNode(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Deletes the Service with wsdl=@wsdl Returns true if deletion is successfull, false otherwise.
     *
     * @param wsdl Service WSDL
     * @return
     */
    public boolean deleteService(String wsdl) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
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
     * Deletes the SharedDisk with name=@name Returns true if deletion is successful, false otherwise.
     *
     * @param name Cloud provider name
     * @return
     */
    public boolean deleteCloudProvider(String name) {
        List<Object> objList = this.resources.getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            for (int i = 0; i < objList.size(); ++i) {
                Object obj = objList.get(i);
                if (obj instanceof CloudProviderType) {
                    CloudProviderType cp = (CloudProviderType) obj;
                    if (cp.getName().equals(name)) {
                        objList.remove(i);
                        return true;
                    }
                }
            }
        }

        return false;
    }

}
