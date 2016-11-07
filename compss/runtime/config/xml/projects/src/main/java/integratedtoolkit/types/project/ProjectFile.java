package integratedtoolkit.types.project;

import java.io.File;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.logging.log4j.Logger;
import org.xml.sax.SAXException;

import integratedtoolkit.types.project.exceptions.ProjectFileValidationException;
import integratedtoolkit.types.project.exceptions.InvalidElementException;
import integratedtoolkit.types.project.jaxb.*;


public class ProjectFile {

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


    /*
     * ************************************** 
     * CONSTRUCTORS
     **************************************/
    /**
     * Creates a ProjectFile instance from a given XML file. The XML is validated against the given schema and
     * internally validated
     *
     * @param xml
     * @param xsd
     * @throws JAXBException
     * @throws ProjectFileValidationException
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
     * @param xmlString
     * @param xsd
     * @throws JAXBException
     * @throws ProjectFileValidationException
     */
    public ProjectFile(String xmlString, Schema xsd, Logger logger) throws JAXBException, ProjectFileValidationException {
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
     * @param xml
     * @param xsdPath
     * @throws SAXException
     * @throws JAXBException
     * @throws ProjectFileValidationException
     */
    public ProjectFile(File xml, String xsdPath, Logger logger) throws SAXException, JAXBException, ProjectFileValidationException {
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
     * @param xmlString
     * @param xsdPath
     * @throws SAXException
     * @throws JAXBException
     * @throws ProjectFileValidationException
     */
    public ProjectFile(String xmlString, String xsdPath, Logger logger) throws SAXException, JAXBException, ProjectFileValidationException {
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

    public ProjectFile(String xsdPath, Logger logger) throws SAXException, JAXBException, ProjectFileValidationException {
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

    private void addEmptyMaster() {
        MasterNodeType master = new MasterNodeType();
        project.getMasterNodeOrComputeNodeOrDataNode().add(master);

    }

    /*
     * ************************************** 
     * DUMPERS
     **************************************/
    /**
     * Stores the current Project object to the given file
     *
     * @param file
     * @throws JAXBException
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
     * Returns the string construction of the current Project
     *
     * @return
     * @throws JAXBException
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

    /*
     * ************************************** 
     * GETTERS: MAIN ELEMENTS LISTS
     **************************************/
    /**
     * Returns the JAXB class representing the all XML file content
     *
     * @return
     */
    public ProjectType getProject() {
        return this.project;
    }

    /**
     * Returns the instance of the MasterNode. Null if not found (but XSD schema doesn't allow it)
     *
     * @return
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
     * Returns a list of declared ComputeNodes
     *
     * @return
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
     * Returns a list of declared DataNodes
     *
     * @return
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
     * Returns a list of declared Services
     *
     * @return
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
     * Returns a list of declared CloudProviders
     *
     * @return
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

    /*
     * ************************************** 
     * GETTERS: MAIN ELEMENTS HASH-MAPS
     **************************************/
    /**
     * Returns a HashMap of declared ComputeNodes (Key: Name, Value: CN)
     *
     * @return
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
     * Returns a HashMap of declared DataNodes (Key: Name, Value: DN)
     *
     * @return
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
     * Returns a HashMap of declared Services (Key: WSDL, Value: Service)
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
     * Returns a HashMap of declared CloudProviders (Key: Name, Value: CP)
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

    /*
     * **************************************
     * GETTERS: MAIN ELEMENTS KEY-VALUES (NAME)
     **************************************/
    /**
     * Returns a List of the names of the declared ComputeNodes
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
     * Returns a List of the names of the declared DataNodes
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
     * Returns a List of the names of the declared Services
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
     * Returns a List of the names of the declared CloudProviders
     *
     * @return
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

    /*
     * ************************************** 
     * GETTERS: MAIN ELEMENTS SINGLE
     **************************************/
    /**
     * Returns the ComputeNode with name = @name. Null if name doesn't exist
     *
     * @param name
     * @return
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
     * Returns the user of a given ComputeNode. Null if not defined
     *
     * @param cn
     * @return
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
     * Returns the installDir of a given ComputeNode. Null if not defined
     *
     * @param cn
     * @return
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
     * Returns the workingDir of a given ComputeNode. Null if not defined
     *
     * @param cn
     * @return
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
     * Returns the application information of a given ComputeNode. Null if not defined
     *
     * @param cn
     * @return
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
     * Returns the limitOfTasks of a given ComputeNode. -1 if not defined
     *
     * @param cn
     * @return
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
     * Returns the queues of a given Adaptor within a given ComputeNode
     *
     * @param cn
     * @param adaptorName
     * @return
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
     * Returns the declared properties of a given Adaptor within a given ComputeNode
     *
     * @param cn
     * @param adaptorName
     * @return
     */
    public Object getAdaptorProperties(ComputeNodeType cn, String adaptorName) {
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
     * Returns the DataNode with name = @name. Null if name doesn't exist
     *
     * @param name
     * @return
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
     * Returns the Service with wsdl = @wsdl. Null if name doesn't exist
     *
     * @param name
     * @return
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
     * Returns the CloudProvider with name = @name. Null if name doesn't exist
     *
     * @param name
     * @return
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
     * Returns the initial number of VMs declared on a given cloud
     *
     * @param c
     * @return
     */
    public int getInitialVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("InitialVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return -1;
    }

    /**
     * Returns the minimum number of VMs declared on a given cloud
     *
     * @param c
     * @return
     */
    public int getMinVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("MinimumVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return -1;
    }

    /**
     * Returns the maximum number of VMs declared on a given cloud
     *
     * @param c
     * @return
     */
    public int getMaxVMs(CloudType c) {
        List<JAXBElement<?>> elements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
        if (elements != null) {
            for (JAXBElement<?> elem : elements) {
                if (elem.getName().equals(new QName("MaximumVMs"))) {
                    return (Integer) elem.getValue();
                }
            }
        }

        return -1;
    }

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

    /**
     * Returns the queues of a given Adaptor within a given Image
     *
     * @param image
     * @param adaptorName
     * @return
     */
    public List<String> getAdaptorQueues(ImageType image, String adaptorName) {
        List<String> empty_queues = new ArrayList<>();

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
                            return empty_queues; // Empty
                        }
                    } else {
                        return empty_queues; // Empty
                    }
                }
            }
        }

        return empty_queues; // Empty
    }

    /**
     * Returns the declared properties of a given Adaptor within a given Image
     *
     * @param image
     * @param adaptorName
     * @return
     */
    public Object getAdaptorProperties(ImageType image, String adaptorName) {
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
     * Returns the adaptor queues
     *
     * @param adaptor
     * @return
     */
    public List<String> getAdaptorQueues(AdaptorType adaptor) {
        List<String> empty_queues = new ArrayList<>();

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
                                    return empty_queues; // Empty
                                }
                            }
                        }
                    } else {
                        return empty_queues; // Empty
                    }
                }
            }
        }

        return empty_queues; // Empty
    }

    /**
     * Returns the adaptor properties
     *
     * @param adaptor
     * @return
     */
    public Object getAdaptorProperties(AdaptorType adaptor) {
        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            // Loop for submission system
            for (JAXBElement<?> adaptorElement : innerElements) {
                if (adaptorElement.getName().equals(new QName("Ports")) || adaptorElement.getName().equals(new QName("BrokerAdaptor"))
                        || adaptorElement.getName().equals(new QName("Properties"))) {

                    return (Object) adaptorElement.getValue();
                }
            }
        }

        return null;
    }

    /*
     * ************************************** 
     * ADDERS: MAIN ELEMENTS
     **************************************/
    /**
     * Adds the given ComputeNodeType @cn to the project file
     *
     * @param cn
     * @return
     * @throws InvalidElementException
     */
    public ComputeNodeType addComputeNode(ComputeNodeType cn) throws InvalidElementException {
        validator.validateComputeNode(cn);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(cn);
        return cn;
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode
     *
     * @param name
     * @param installDir
     * @param workingDir
     * @param user
     * @param app
     * @param limitOfTasks
     * @param adaptors
     * @return
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, ApplicationType app,
            int limitOfTasks, AdaptorsListType adaptors) throws InvalidElementException {

        ComputeNodeType cn = new ComputeNodeType();
        cn.setName(name);

        // Mandatory elements
        JAXBElement<String> installDir_jaxb = new JAXBElement<String>(new QName("InstallDir"), String.class, installDir);
        cn.getInstallDirOrWorkingDirOrUser().add(installDir_jaxb);
        JAXBElement<String> workingDir_jaxb = new JAXBElement<String>(new QName("WorkingDir"), String.class, workingDir);
        cn.getInstallDirOrWorkingDirOrUser().add(workingDir_jaxb);

        // Non mandatory elements
        if (user != null) {
            JAXBElement<String> user_jaxb = new JAXBElement<String>(new QName("User"), String.class, user);
            cn.getInstallDirOrWorkingDirOrUser().add(user_jaxb);
        }
        if (app != null) {
            JAXBElement<ApplicationType> apps_jaxb = new JAXBElement<ApplicationType>(new QName("Application"), ApplicationType.class, app);
            cn.getInstallDirOrWorkingDirOrUser().add(apps_jaxb);
        }
        if (limitOfTasks >= 0) {
            JAXBElement<Integer> limitOfTasks_jaxb = new JAXBElement<Integer>(new QName("LimitOfTasks"), Integer.class, limitOfTasks);
            cn.getInstallDirOrWorkingDirOrUser().add(limitOfTasks_jaxb);
        }
        if (adaptors != null) {
            JAXBElement<AdaptorsListType> adaptors_jaxb = new JAXBElement<AdaptorsListType>(new QName("Adaptors"), AdaptorsListType.class,
                    adaptors);
            cn.getInstallDirOrWorkingDirOrUser().add(adaptors_jaxb);
        }

        return addComputeNode(cn);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode
     *
     * @param name
     * @param installDir
     * @param workingDir
     * @param user
     * @return
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user) throws InvalidElementException {
        return addComputeNode(name, installDir, workingDir, user, null, -1, (AdaptorsListType) null);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode
     *
     * @param name
     * @param installDir
     * @param workingDir
     * @param user
     * @param app
     * @param limitOfTasks
     * @param adaptors
     * @return
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, ApplicationType app,
            int limitOfTasks, List<AdaptorType> adaptors) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return addComputeNode(name, installDir, workingDir, user, app, limitOfTasks, adaptorsList);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode
     *
     * @param name
     * @param installDir
     * @param workingDir
     * @param user
     * @param appDir
     * @param libPath
     * @param cp
     * @param pypath
     * @param limitOfTasks
     * @param adaptors
     * @return
     * @throws InvalidElementException
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, String appDir, String libPath,
            String cp, String pypath, int limitOfTasks, List<AdaptorType> adaptors) throws InvalidElementException {

        AdaptorsListType adaptorsList = new AdaptorsListType();
        if (adaptors != null) {
            for (AdaptorType a : adaptors) {
                adaptorsList.getAdaptor().add(a);
            }
        }

        return addComputeNode(name, installDir, workingDir, user, appDir, libPath, cp, pypath, limitOfTasks, adaptorsList);
    }

    /**
     * Adds a new ComputeNode with the given information and returns the instance of the new ComputeNode
     *
     * @param name
     * @param installDir
     * @param workingDir
     * @param user
     * @param appDir
     * @param libPath
     * @param cp
     * @param pypath
     * @param limitOfTasks
     * @param adaptors
     * @return
     * @throws InvalidElementException
     */
    public ComputeNodeType addComputeNode(String name, String installDir, String workingDir, String user, String appDir, String libPath,
            String cp, String pypath, int limitOfTasks, AdaptorsListType adaptors) throws InvalidElementException {

        ApplicationType app = createApplication(appDir, libPath, cp, pypath);

        return addComputeNode(name, installDir, workingDir, user, app, limitOfTasks, adaptors);
    }

    /**
     * Adds the given DataNode @dn to the project file
     *
     * @param dn
     * @return
     * @throws InvalidElementException
     */
    public DataNodeType addDataNode(DataNodeType dn) throws InvalidElementException {
        validator.validateDataNode(dn);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(dn);
        return dn;
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode
     *
     * @param name
     * @return
     * @throws InvalidElementException
     */
    public DataNodeType addDataNode(String name) throws InvalidElementException {
        DataNodeType dn = new DataNodeType();
        dn.setName(name);

        return addDataNode(dn);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode
     *
     * @param name
     * @return
     * @throws InvalidElementException
     */
    public DataNodeType addDataNode(String name, AdaptorsListType adaptors) throws InvalidElementException {
        DataNodeType dn = new DataNodeType();
        dn.setName(name);
        dn.getAdaptors().add(adaptors);

        return addDataNode(dn);
    }

    /**
     * Adds a new DataNode with the given information and returns the instance of the new DataNode
     *
     * @param name
     * @return
     * @throws InvalidElementException
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
     * Adds the given Service @s to the project file
     *
     * @param s
     * @return
     * @throws InvalidElementException
     */
    public ServiceType addService(ServiceType s) throws InvalidElementException {
        validator.validateService(s);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(s);
        return s;
    }

    /**
     * Adds a new Service with the given information and returns the instance of the new Service
     *
     * @param wsdl
     * @return
     * @throws InvalidElementException
     */
    public ServiceType addService(String wsdl) throws InvalidElementException {
        ServiceType s = new ServiceType();
        s.setWsdl(wsdl);

        return addService(s);
    }

    /**
     * Adds a new Service with the given information and returns the instance of the new Service
     *
     * @param wsdl
     * @param limitOfTasks
     * @return
     * @throws InvalidElementException
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
     * Adds the given Cloud @c to the project file
     *
     * @param c
     * @return
     * @throws InvalidElementException
     */
    public CloudType addCloud(CloudType c) throws InvalidElementException {
        validator.validateCloud(c);

        // If we reach this point means that the SharedDisk is valid
        this.project.getMasterNodeOrComputeNodeOrDataNode().add(c);
        return c;
    }

    /**
     * Adds a new Cloud with the given information and returns the instance of the new Cloud
     *
     * @param cps
     * @return
     * @throws InvalidElementException
     */
    public CloudType addCloud(List<CloudProviderType> cps) throws InvalidElementException {
        CloudType c = new CloudType();

        // Mandatory elements
        if (cps != null) {
            for (CloudProviderType cp : cps) {
                JAXBElement<CloudProviderType> cp_jaxb = new JAXBElement<CloudProviderType>(new QName("CloudProvider"),
                        CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cp_jaxb);
            }
        }

        return addCloud(c);
    }

    /**
     * Adds a new Cloud with the given information and returns the instance of the new Cloud
     *
     * @param cps
     * @param initialVMs
     * @param minVMs
     * @param maxVMs
     * @return
     * @throws InvalidElementException
     */
    public CloudType addCloud(List<CloudProviderType> cps, int initialVMs, int minVMs, int maxVMs) throws InvalidElementException {
        CloudType c = new CloudType();

        // Mandatory elements
        if (cps != null) {
            for (CloudProviderType cp : cps) {
                JAXBElement<CloudProviderType> cp_jaxb = new JAXBElement<CloudProviderType>(new QName("CloudProvider"),
                        CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cp_jaxb);
            }
        }

        // Optional parameters
        if (initialVMs >= 0) {
            JAXBElement<Integer> initialVMs_jaxb = new JAXBElement<Integer>(new QName("InitialVMs"), Integer.class, initialVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(initialVMs_jaxb);
        }
        if (minVMs >= 0) {
            JAXBElement<Integer> minVMs_jaxb = new JAXBElement<Integer>(new QName("MinimumVMs"), Integer.class, minVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(minVMs_jaxb);
        }
        if (maxVMs >= 0) {
            JAXBElement<Integer> maxVMs_jaxb = new JAXBElement<Integer>(new QName("MaximumVMs"), Integer.class, maxVMs);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(maxVMs_jaxb);
        }

        return addCloud(c);
    }

    /**
     * Adds the given CloudProvider @cp to the project file
     *
     * @param cp
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(CloudProviderType cp) throws InvalidElementException {
        validator.validateCloudProvider(cp);

        // If we reach this point means that the SharedDisk is valid
        boolean cloudTagFound = false;
        for (Object obj : this.project.getMasterNodeOrComputeNodeOrDataNode()) {
            if (obj instanceof CloudType) {
                cloudTagFound = true;
                CloudType c = (CloudType) obj;
                JAXBElement<CloudProviderType> cp_jaxb = new JAXBElement<CloudProviderType>(new QName("CloudProvider"),
                        CloudProviderType.class, cp);
                c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cp_jaxb);
            }
        }

        if (!cloudTagFound) {
            // Create the cloud tag with default values (none)
            CloudType c = new CloudType();
            this.project.getMasterNodeOrComputeNodeOrDataNode().add(c);
            // Add the requested provider
            JAXBElement<CloudProviderType> cp_jaxb = new JAXBElement<CloudProviderType>(new QName("CloudProvider"), CloudProviderType.class,
                    cp);
            c.getCloudProviderOrInitialVMsOrMinimumVMs().add(cp_jaxb);

        }
        return cp;
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, InstanceTypesType instances) throws InvalidElementException {
        return addCloudProvider(name, images, instances, -1, null);
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @return
     * @throws InvalidElementException
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
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @return
     * @throws InvalidElementException
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
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @return
     * @throws InvalidElementException
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
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @param limitOfVMs
     * @param properties
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, InstanceTypesType instances, int limitOfVMs,
            CloudPropertiesType properties) throws InvalidElementException {
        
        ImagesType imagesList = new ImagesType();
        if (images != null) {
            for (ImageType im : images) {
                imagesList.getImage().add(im);
            }
        }

        return addCloudProvider(name, imagesList, instances, limitOfVMs, properties);
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @param limitOfVMs
     * @param properties
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, List<InstanceTypeType> instances, int limitOfVMs,
            CloudPropertiesType properties) throws InvalidElementException {
        
        InstanceTypesType instancesList = new InstanceTypesType();
        if (instances != null) {
            for (InstanceTypeType ins : instances) {
                instancesList.getInstanceType().add(ins);
            }
        }

        return addCloudProvider(name, images, instancesList, limitOfVMs, properties);
    }

    /**
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @param limitOfVMs
     * @param properties
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(String name, List<ImageType> images, List<InstanceTypeType> instances, int limitOfVMs,
            CloudPropertiesType properties) throws InvalidElementException {
        
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
     * Adds a new CloudProvider with the given information and returns the instance of the new CloudProvider
     *
     * @param name
     * @param images
     * @param instances
     * @param limitOfVMs
     * @param properties
     * @return
     * @throws InvalidElementException
     */
    public CloudProviderType addCloudProvider(String name, ImagesType images, InstanceTypesType instances, int limitOfVMs,
            CloudPropertiesType properties) throws InvalidElementException {

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

    /*
     * **************************************
     * SETTERS: HELPERS FOR SECOND LEVEL ELEMENTS
     **************************************/
    /**
     * Sets the general cloud properties to an existing cloud Returns true if all modifications have been performed,
     * false otherwise
     *
     * @param initialVMs
     * @param minVMs
     * @param maxVMs
     * @return
     * @throws InvalidElementException
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
                JAXBElement<Integer> initialVMs_jaxb = new JAXBElement<Integer>(new QName("InitialVMs"), Integer.class, initialVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(initialVMs_jaxb);
            }
            if (minVMs >= 0) {
                JAXBElement<Integer> minVMs_jaxb = new JAXBElement<Integer>(new QName("MinimumVMs"), Integer.class, minVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(minVMs_jaxb);
            }
            if (maxVMs >= 0) {
                JAXBElement<Integer> maxVMs_jaxb = new JAXBElement<Integer>(new QName("MaximumVMs"), Integer.class, maxVMs);
                cloud.getCloudProviderOrInitialVMsOrMinimumVMs().add(maxVMs_jaxb);
            }
        }

        return true;
    }

    /**
     * Adds the given image @image to the cloudProvider with name =
     * 
     * @cloudProviderName Returns true if image is inserted, false otherwise
     *
     * @param cloudProviderName
     * @param image
     * @return
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
                // No InstanceTypes tag found, create it and add
                ImagesType images = new ImagesType();
                images.getImage().add(image);
                objList.add(images);
            }
            return false;
        } else {
            return false;
        }
    }

    /**
     * Adds the given instance @instance to the cloudProvider with name =
     * 
     * @cloudProviderName Returns true if instance is inserted, false otherwise
     *
     * @param cloudProviderName
     * @param instance
     * @return
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
            }
            return false;
        } else {
            return false;
        }
    }

    public static ApplicationType createApplication(String appDir, String libPath, String cp, String pypath) {
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
        return app;

    }

    public static ImageType createImage(String name, String installDir, String workingDir, String user, String appDir, String libPath,
            String cp, String pypath, int limitOfTasks, PackageType pack, AdaptorsListType adaptors) {
        
        ImageType image = new ImageType();

        // Mandatory elements
        image.setName(name);
        JAXBElement<String> installDir_jaxb = new JAXBElement<String>(new QName("InstallDir"), String.class, installDir);
        image.getInstallDirOrWorkingDirOrUser().add(installDir_jaxb);
        JAXBElement<String> workingDir_jaxb = new JAXBElement<String>(new QName("WorkingDir"), String.class, workingDir);
        image.getInstallDirOrWorkingDirOrUser().add(workingDir_jaxb);

        // Non mandatory elements
        if (user != null) {
            JAXBElement<String> user_jaxb = new JAXBElement<String>(new QName("User"), String.class, user);
            image.getInstallDirOrWorkingDirOrUser().add(user_jaxb);
        }
        ApplicationType app = createApplication(appDir, libPath, cp, pypath);
        JAXBElement<ApplicationType> apps_jaxb = new JAXBElement<ApplicationType>(new QName("Application"), ApplicationType.class, app);
        image.getInstallDirOrWorkingDirOrUser().add(apps_jaxb);
        if (limitOfTasks >= 0) {
            JAXBElement<Integer> limitOfTasks_jaxb = new JAXBElement<Integer>(new QName("LimitOfTasks"), Integer.class, limitOfTasks);
            image.getInstallDirOrWorkingDirOrUser().add(limitOfTasks_jaxb);
        }
        if (pack != null) {
            JAXBElement<PackageType> pack_jaxb = new JAXBElement<PackageType>(new QName("Package"), PackageType.class, pack);
            image.getInstallDirOrWorkingDirOrUser().add(pack_jaxb);
        }
        if (adaptors != null) {
            JAXBElement<AdaptorsListType> adaptors_jaxb = new JAXBElement<AdaptorsListType>(new QName("Adaptors"), AdaptorsListType.class,
                    adaptors);
            image.getInstallDirOrWorkingDirOrUser().add(adaptors_jaxb);
        }
        return image;

    }

    public static InstanceTypeType createInstance(String name) {
        InstanceTypeType instance = new InstanceTypeType();
        instance.setName(name);
        return instance;

    }

    /**
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param subsys
     * @param nioproperties
     * @param user
     * @return
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, NIOAdaptorProperties nioproperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"),
                SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<NIOAdaptorProperties> propertiesElement = new JAXBElement<NIOAdaptorProperties>(new QName("Ports"),
                NIOAdaptorProperties.class, nioproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param subsys
     * @param gatproperties
     * @param user
     * @return
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, String gatproperties, String user) {
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"),
                SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<String> propertiesElement = new JAXBElement<String>(new QName("BrokerAdaptor"), String.class, gatproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param subsys
     * @param externalproperties
     * @param user
     * @return
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, ExternalAdaptorProperties externalproperties,
            String user) {
        
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"),
                SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        JAXBElement<ExternalAdaptorProperties> propertiesElement = new JAXBElement<ExternalAdaptorProperties>(new QName("Properties"),
                ExternalAdaptorProperties.class, externalproperties);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param subsys
     * @param externalProperties
     * @param user
     * @return
     */
    public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, List<PropertyAdaptorType> externalProperties,
            String user) {
        
        AdaptorType adaptor = new AdaptorType();
        adaptor.setName(name);

        JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(new QName("SubmissionSystem"),
                SubmissionSystemType.class, subsys);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

        ExternalAdaptorProperties externalproperties_list = new ExternalAdaptorProperties();
        if (externalProperties != null) {
            for (PropertyAdaptorType pa : externalProperties) {
                externalproperties_list.getProperty().add(pa);
            }
        }
        JAXBElement<ExternalAdaptorProperties> propertiesElement = new JAXBElement<ExternalAdaptorProperties>(new QName("Properties"),
                ExternalAdaptorProperties.class, externalproperties_list);
        adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(propertiesElement);

        // Optional parameters
        if (user != null) {
            JAXBElement<String> userElement = new JAXBElement<String>(new QName("User"), String.class, user);
            adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(userElement);
        }

        return adaptor;
    }

    /**
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param batch
     * @param queues
     * @param interactive
     * @param nioproperties
     * @param user
     * @return
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
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param batch
     * @param queues
     * @param interactive
     * @param gatproperties
     * @param user
     * @return
     */
    public static AdaptorType createAdaptor(String name, boolean batch, List<String> queues, boolean interactive, String gatproperties,
            String user) {
        
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
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param batch
     * @param queues
     * @param interactive
     * @param externalProperties
     * @param user
     * @return
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
     * Creates an instance of an Adaptor with the given information
     *
     * @param name
     * @param batch
     * @param queues
     * @param interactive
     * @param externalProperties
     * @param user
     * @return
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
            JAXBElement<InteractiveType> interactiveElement = new JAXBElement<InteractiveType>(new QName("Interactive"),
                    InteractiveType.class, i);
            subsys.getBatchOrInteractive().add(interactiveElement);
        }

        return createAdaptor(name, subsys, externalProperties, user);
    }

    /*
     * ************************************** 
     * DELETERS: MAIN ELEMENTS
     **************************************/
    /**
     * Deletes the MasterNode Returns true if deletion is successfull, false otherwise
     *
     * @return
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
     * Deletes the ComputeNode with name = @name Returns true if deletion is successfull, false otherwise
     *
     * @param name
     * @return
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
     * Deletes the DataNode with name = @name Returns true if deletion is successfull, false otherwise
     *
     * @param name
     * @return
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
     * Deletes the Service with wsdl = @wsdl Returns true if deletion is successfull, false otherwise
     *
     * @param name
     * @return
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
     * Deletes the Cloud Returns true if deletion is successfull, false otherwise
     *
     * @return
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
     * Deletes the CloudProvider with name = @name Returns true if deletion is successfull, false otherwise
     *
     * @param name
     * @return
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

}
