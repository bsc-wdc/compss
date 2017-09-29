package es.bsc.compss.types.resources;

import es.bsc.compss.types.resources.exceptions.InvalidElementException;
import es.bsc.compss.types.resources.exceptions.ResourcesFileValidationException;
import es.bsc.compss.types.resources.jaxb.*;

import java.io.File;
import java.io.Serializable;
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

import org.xml.sax.SAXException;
import org.apache.logging.log4j.Logger;

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
	 * Creates a ResourceFile instance from a given XML file. The XML is validated
	 * against the given schema and internally validated
	 *
	 * @param xml
	 * @param xsd
	 * @throws JAXBException
	 * @throws ResourcesFileValidationException
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

	public ResourcesFile(Logger logger) throws JAXBException {
		this.logger = logger;
		this.context = JAXBContext.newInstance(ObjectFactory.class.getPackage().getName());
		this.validator = new Validator(this, this.logger);
		this.resources = new ResourcesListType();
	}

	/**
	 * Creates a ResourceFile instance from a given XML string. The XML is validated
	 * against the given schema and internally validated
	 *
	 * @param xmlString
	 * @param xsd
	 * @throws JAXBException
	 * @throws ResourcesFileValidationException
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
	 * Creates a ResourceFile instance from a given XML file. The XML is validated
	 * against the given path of the schema and internally validated
	 *
	 * @param xml
	 * @param xsdPath
	 * @throws SAXException
	 * @throws JAXBException
	 * @throws ResourcesFileValidationException
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
	 * Creates a ResourceFile instance from a given XML string. The XML is validated
	 * against the given path of the schema and internally validated
	 *
	 * @param xmlString
	 * @param xsdPath
	 * @throws SAXException
	 * @throws JAXBException
	 * @throws ResourcesFileValidationException
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
	 * Create an empty resourceFile object
	 * 
	 * @param xsdPath
	 * @param logger
	 * @throws SAXException
	 * @throws JAXBException
	 * @throws ResourcesFileValidationException
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

	/*
	 * ************************************** DUMPERS
	 **************************************/
	/**
	 * Stores the current ResourceList object to the given file
	 *
	 * @param file
	 * @throws JAXBException
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
	 * Returns the string construction of the current ResourceList
	 *
	 * @return
	 * @throws JAXBException
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
	 * ************************************** GETTERS: MAIN ELEMENTS LISTS
	 **************************************/
	/**
	 * Returns the JAXB class representing the all XML file content
	 *
	 * @return
	 */
	public ResourcesListType getResources() {
		return this.resources;
	}

	/**
	 * Returns a list of declared Shared Disks
	 *
	 * @return
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
	 * Returns a list of declared DataNodes
	 *
	 * @return
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
	 * Returns a list of declared ComputeNodes
	 *
	 * @return
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
	 * Returns a list of declared Services
	 *
	 * @return
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
	 * Returns a list of declared CloudProviders
	 *
	 * @return
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

	/*
	 * ************************************** GETTERS: MAIN ELEMENTS HASH-MAPS
	 **************************************/
	/**
	 * Returns a HashMap of declared Shared Disks (Key: Name, Value: SD)
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
	 * Returns a HashMap of declared DataNodes (Key: Name, Value: DN)
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
	 * Returns a HashMap of declared ComputeNodes (Key: Name, Value: CN)
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
	 * Gets the mount points defined in the different compute nodes
	 * 
	 * @param diskName
	 *            Name of the disk
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
					if (disks.containsKey(diskName)) {
						mountPoints.put(cn.getName(), disks.get(diskName));
					}
				}
			}
		}

		return mountPoints;
	}

	/**
	 * Returns a HashMap of declared Services (Key: WSDL, Value: Service)
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
	 * Returns a HashMap of declared Shared Disks (Key: Name, Value: List<Services>)
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
	 * Returns a HashMap of declared CloudProviders (Key: Name, Value: CP)
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

	/*
	 * ************************************** GETTERS: MAIN ELEMENTS KEY-VALUES
	 * (NAME)
	 **************************************/
	/**
	 * Returns a List of the names of the declared SharedDisks
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
	 * Returns a List of the names of the declared DataNodes
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
	 * Returns a List of the names of the declared ComputeNodes
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
	 * Returns a List of the WSDLs of the declared Services
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
	 * Returns a List of the names of the declared Services (without repetition)
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
	 * Returns a List of the names of the declared CloudProviders
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

	/*
	 * ************************************** GETTERS: MAIN ELEMENTS SINGLE
	 **************************************/
	/**
	 * Returns the SharedDisk with name=@name. Null if name doesn't exist
	 *
	 * @param name
	 * @return
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
	 * @param name
	 * @return
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
	 * Returns the Host of a given DataNode
	 *
	 * @param d
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
	 * Returns the path of a given DataNode
	 *
	 * @param d
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
	 * Returns the storage size of a given DataNode
	 *
	 * @param d
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
	 * Returns the storage type of a given DataNode
	 *
	 * @param d
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
	 * Returns the ComputeNode with name=@name. Null if name doesn't exist
	 *
	 * @param name
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
	 * Returns the processors of a given Compute Node @c
	 *
	 * @param c
	 * @return
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
	 * Returns the memory size of a given ComputeNode
	 *
	 * @param c
	 * @return
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
	 * Returns the memory type of a given ComputeNode
	 *
	 * @param c
	 * @return
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
	 * Returns the storage size of a given ComputeNode
	 *
	 * @param c
	 * @return
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
	 * Returns the storage type of a given ComputeNode
	 *
	 * @param c
	 * @return
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
	 * Returns the OperatingSystem properties of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public OSType getOperatingSystem(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
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
	 * Returns the OperatingSystem type of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public String getOperatingSystemType(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = ((OSType) obj);
					return getOperatingSystemType(os);
				}
			}
		}

		return null;
	}

	/**
	 * Returns the OperatingSystem distribution of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public String getOperatingSystemDistribution(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = ((OSType) obj);
					return getOperatingSystemDistribution(os);
				}
			}
		}

		return null;
	}

	/**
	 * Returns the OperatingSystem Version of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public String getOperatingSystemVersion(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = ((OSType) obj);
					return getOperatingSystemVersion(os);
				}
			}
		}

		return null;
	}

	/**
	 * Returns the applications of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public List<String> getApplications(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
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
	 * Returns the price properties of a given ComputeNode
	 *
	 * @param c
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
	 * Returns the SharedDisks (name, mountpoint) of a given ComputeNode
	 *
	 * @param c
	 * @return
	 */
	public HashMap<String, String> getSharedDisks(ComputeNodeType c) {
		List<Object> objList = c.getProcessorOrAdaptorsOrMemory();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof AttachedDisksListType) {
					AttachedDisksListType disks = (AttachedDisksListType) obj;
					HashMap<String, String> disksInformation = new HashMap<>();
					for (AttachedDiskType disk : disks.getAttachedDisk()) {
						disksInformation.put(disk.getName(), disk.getMountPoint());
					}
					return disksInformation;
				}
			}
		}

		return null;
	}

	/**
	 * Returns the AttacSharedDisks of a given ComputeNode
	 *
	 * @param c
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
	 * Returns the queues of a given Adaptor within a given ComputeNode
	 *
	 * @param cn
	 * @param adaptorName
	 * @return
	 */
	public List<String> getAdaptorQueues(ComputeNodeType cn, String adaptorName) {
		List<String> empty_queues = new ArrayList<>();

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
						return empty_queues; // Empty
					}
				}
			}
		}

		return empty_queues; // Empty
	}

	/**
	 * Returns the declared properties of a given Adaptor within a given ComputeNode
	 *
	 * @param cn
	 * @param adaptorName
	 * @return
	 */
	public Object getAdaptorProperties(ComputeNodeType cn, String adaptorName) {
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
	 * Returns the declared properties of a given Adaptor within a given ComputeNode
	 *
	 * @param cn
	 * @param adaptorName
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
	 * Returns the Service with wsdl=@wsdl. Null if name doesn't exist
	 *
	 * @param wsdl
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
	 * Returns the CloudProvider with name=@name. Null if name doesn't exist
	 *
	 * @param name
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
	 * Returns the instance with the given name within the given CloudProvider. Null
	 * if not found
	 *
	 * @param cp
	 * @param instanceName
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
	 * Returns the image with the given name within the given CloudProvider. Null if
	 * not found
	 *
	 * @param cp
	 * @param imageName
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
	 * Returns the connector main class information form a given endpoint
	 *
	 * @param endpoint
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
	 * Returns the connector jar file information form a given endpoint
	 *
	 * @param endpoint
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
	 * Returns the server information form a given endpoint
	 *
	 * @param endpoint
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
	 * Returns the port information form a given endpoint
	 *
	 * @param endpoint
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
	 * Returns the OS Type associated to a given Image
	 *
	 * @param image
	 * @return
	 */
	public String getOperatingSystemType(ImageType image) {
		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = (OSType) obj;
					return getOperatingSystemType(os);
				}
			}
		}
		return null;
	}

	/**
	 * Returns the OS Distribution associated to a given Image
	 *
	 * @param image
	 * @return
	 */
	public String getOperatingSystemDistribution(ImageType image) {
		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = (OSType) obj;
					return getOperatingSystemDistribution(os);
				}
			}
		}
		return null;
	}

	/**
	 * Returns the OS Version associated to a given Image
	 *
	 * @param image
	 * @return
	 */
	public String getOperatingSystemVersion(ImageType image) {
		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
		if (objList != null) {
			for (Object obj : objList) {
				if (obj instanceof OSType) {
					OSType os = (OSType) obj;
					return getOperatingSystemVersion(os);
				}
			}
		}
		return null;
	}

	/**
	 * Returns the OS type
	 *
	 * @param os
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
	 * Returns the OS Distribution
	 *
	 * @param os
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
	 * Returns the OS Version
	 *
	 * @param os
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
	 * Returns the applications associated to a given image
	 *
	 * @param image
	 * @return
	 */
	public List<String> getApplications(ImageType image) {
		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
		if (objList != null) {
			// Loop for adaptors tag
			for (Object obj : objList) {
				if (obj instanceof SoftwareListType) {
					return ((SoftwareListType) obj).getApplication();
				}
			}
		}

		return null;
	}

	/**
	 * Returns the price information associated to a given image
	 *
	 * @param image
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
	 * Returns the image creation time associated to a given image
	 *
	 * @param image
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

	/**
	 * Returns the shared Disks associated to a given image
	 *
	 * @param image
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
	 * Returns the queues of a given Adaptor within a given Image
	 *
	 * @param image
	 * @param adaptorName
	 * @return
	 */
	public List<String> getAdaptorQueues(ImageType image, String adaptorName) {
		List<String> empty_queues = new ArrayList<>();

		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
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
		List<Object> objList = image.getAdaptorsOrOperatingSystemOrSoftware();
		if (objList != null) {
			// Loop for adaptors tag
			for (Object obj : objList) {
				if (obj instanceof AdaptorsListType) {
					List<AdaptorType> adaptors = ((AdaptorsListType) obj).getAdaptor();
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
	 * Returns the processors of a given InstanceType
	 *
	 * @param instance
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
	 * Returns the memory size of a given InstanceType
	 *
	 * @param instance
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
	 * Returns the memory type of a given InstanceType
	 *
	 * @param instance
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
	 * Returns the storage size of a given InstanceType
	 *
	 * @param instance
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
	 * Returns the storage type of a given InstanceType
	 *
	 * @param instance
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
	 * Returns the price information of a given InstanceType
	 *
	 * @param instance
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
				if (adaptorElement.getName().equals(new QName("Ports"))
						|| adaptorElement.getName().equals(new QName("BrokerAdaptor"))
						|| adaptorElement.getName().equals(new QName("Properties"))) {

					return (Object) adaptorElement.getValue();
				}
			}
		}

		return null;
	}

	/*
	 * ************************************** ADDERS: MAIN ELEMENTS
	 * *************************************
	 */
	/**
	 * Adds the given SharedDisk @sd to the resources file
	 *
	 * @param sd
	 * @return
	 * @throws InvalidElementException
	 */
	public SharedDiskType addSharedDisk(SharedDiskType sd) throws InvalidElementException {
		validator.validateSharedDisk(sd);

		// If we reach this point means that the SharedDisk is valid
		this.resources.getSharedDiskOrDataNodeOrComputeNode().add(sd);
		return sd;
	}

	/**
	 * Adds a new SharedDisk with name=@name and returns the instance of the new
	 * SharedDisk
	 *
	 * @param name
	 * @return
	 * @throws InvalidElementException
	 */
	public SharedDiskType addSharedDisk(String name) throws InvalidElementException {
		SharedDiskType sd = new SharedDiskType();
		sd.setName(name);
		sd.setStorage(null);

		return this.addSharedDisk(sd);
	}

	/**
	 * Adds a new SharedDisk with name=@name and storage=@storage and returns the
	 * instance of the new SharedDisk
	 *
	 * @param name
	 * @param storage
	 * @return
	 * @throws InvalidElementException
	 */
	public SharedDiskType addSharedDisk(String name, StorageType storage) throws InvalidElementException {
		SharedDiskType sd = new SharedDiskType();
		sd.setName(name);
		sd.setStorage(storage);

		return this.addSharedDisk(sd);
	}

	/**
	 * Adds the given DataNode @dn to the resources file
	 *
	 * @param dn
	 * @return
	 * @throws InvalidElementException
	 */
	public DataNodeType addDataNode(DataNodeType dn) throws InvalidElementException {
		validator.validateDataNode(dn);

		// If we reach this point means that the DataNode is valid
		this.resources.getSharedDiskOrDataNodeOrComputeNode().add(dn);
		return dn;
	}

	/**
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @return
	 * @throws InvalidElementException
	 */
	public DataNodeType addDataNode(String name, String host, String path, AdaptorsListType adaptors)
			throws InvalidElementException {
		return this.addDataNode(name, host, path, adaptors, null, (AttachedDisksListType) null);
	}

	/**
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @param storage
	 * @param sharedDisks
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @param storage
	 * @param sharedDisks
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @param storage
	 * @param sharedDisks
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new DataNode with the given information and returns the instance of
	 * the new DataNode
	 *
	 * @param name
	 * @param host
	 * @param path
	 * @param adaptors
	 * @param storage
	 * @param sharedDisks
	 * @return
	 * @throws InvalidElementException
	 */
	public DataNodeType addDataNode(String name, String host, String path, AdaptorsListType adaptors,
			StorageType storage, AttachedDisksListType sharedDisks) throws InvalidElementException {

		JAXBElement<String> hostElement = new JAXBElement<String>(new QName("Host"), String.class, host);
		JAXBElement<String> pathElement = new JAXBElement<String>(new QName("Path"), String.class, path);
		JAXBElement<AdaptorsListType> adaptorsElement = new JAXBElement<AdaptorsListType>(new QName("Adaptors"),
				AdaptorsListType.class, adaptors);

		DataNodeType dn = new DataNodeType();
		dn.setName(name);
		dn.getHostOrPathOrAdaptors().add(hostElement);
		dn.getHostOrPathOrAdaptors().add(pathElement);
		dn.getHostOrPathOrAdaptors().add(adaptorsElement);

		// Optional parameters
		if (storage != null) {
			JAXBElement<StorageType> storageElement = new JAXBElement<StorageType>(new QName("Storage"),
					StorageType.class, storage);
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
	 * Adds the given ComputeNode @cn to the resources file
	 *
	 * @param cn
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(ComputeNodeType cn) throws InvalidElementException {
		validator.validateComputeNode(cn);

		// If we reach this point means that the ComputeNode is valid
		this.resources.getSharedDiskOrDataNodeOrComputeNode().add(cn);
		return cn;
	}

	/**
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors)
			throws InvalidElementException {
		return this.addComputeNode(name, processors, adaptors, null, null, null, (SoftwareListType) null,
				(AttachedDisksListType) null, null);
	}

	/**
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @return
	 * @throws InvalidElementException
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
	 * Add a compute node with a single instances of processor and adaptor
	 * 
	 * @param name
	 * @param processor
	 * @param adaptor
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @param memory
	 * @param storage
	 * @param os
	 * @param applications
	 * @param sharedDisks
	 * @param price
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors,
			MemoryType memory, StorageType storage, OSType os, List<String> applications,
			AttachedDisksListType sharedDisks, PriceType price) throws InvalidElementException {

		SoftwareListType software = new SoftwareListType();
		if (applications != null) {
			for (String app : applications) {
				software.getApplication().add(app);
			}
		}

		return this.addComputeNode(name, processors, adaptors, memory, storage, os, software, sharedDisks, price);
	}

	/**
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @param memory
	 * @param storage
	 * @param os
	 * @param software
	 * @param sharedDisks
	 * @param price
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @param memory
	 * @param storage
	 * @param os
	 * @param applications
	 * @param sharedDisks
	 * @param price
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @param memory
	 * @param storage
	 * @param os
	 * @param software
	 * @param sharedDisks
	 * @param price
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(String name, List<ProcessorType> processors, AdaptorsListType adaptors,
			MemoryType memory, StorageType storage, OSType os, SoftwareListType software,
			AttachedDisksListType sharedDisks, PriceType price) throws InvalidElementException {

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
	 * Adds a new ComputeNode with the given information and returns the instance of
	 * the new ComputeNode
	 *
	 * @param name
	 * @param processors
	 * @param adaptors
	 * @param memorySize
	 * @param diskSize
	 * @param osName
	 * @return
	 * @throws InvalidElementException
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
			StorageType storage = createStorage(diskSize, null);
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
	 * Add an instance of compute node with a single processor and a NIO adaptor
	 * 
	 * @param name
	 *            Node name
	 * @param procName
	 *            Processor Name
	 * @param procCU
	 *            Processor Computing Units
	 * @param procArch
	 *            Processor Architecture
	 * @param procSpeed
	 *            Processor Speed
	 * @param procType
	 *            Processor Type
	 * @param procMemSize
	 *            Processor Internal Memory Size
	 * @param procProp
	 *            Processor Property
	 * @param adaptorName
	 *            Adaptor Name
	 * @param maxPort
	 *            Maximum port number of the port range
	 * @param minPort
	 *            Minimum port number of the port range
	 * @param executor
	 *            Executor command
	 * @param user
	 *            Username
	 * @param memorySize
	 *            Memory size
	 * @param memoryType
	 *            Memory type
	 * @param storageSize
	 *            Storage size
	 * @param storageType
	 *            Storage type
	 * @param osType
	 *            Operating system type
	 * @param osDistribution
	 *            Operating system distribution
	 * @param osVersion
	 *            Operating system version
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(String name, String procName, int procCU, String procArch, float procSpeed,
			String procType, float procMemSize, ProcessorPropertyType procProp, String adaptorName, int maxPort,
			int minPort, String executor, String user, float memorySize, String memoryType, float storageSize,
			String storageType, String osType, String osDistribution, String osVersion) throws InvalidElementException {

		List<ProcessorType> processors = new ArrayList<>();
		ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
		processors.add(pr);
		MemoryType mem = createMemory(memorySize, memoryType);
		StorageType storage = createStorage(storageSize, storageType);
		OSType os = createOperatingSystem(osType, osDistribution, osVersion);
		List<AdaptorType> adaptors = new ArrayList<>();
		NIOAdaptorProperties nioProp = new NIOAdaptorProperties();
		nioProp.setMaxPort(maxPort);
		nioProp.setMinPort(minPort);
		nioProp.setRemoteExecutionCommand(executor);
		AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, user);
		adaptors.add(adaptor);
		return this.addComputeNode(name, processors, adaptors, mem, storage, os, null, null, null);
	}

	public ComputeNodeType addComputeNode(String name, String procName, int procCU, String adaptorName, int maxPort,
			int minPort) throws InvalidElementException {
		List<ProcessorType> processors = new ArrayList<>();
		ProcessorType pr = createProcessor(procName, procCU);
		processors.add(pr);
		List<AdaptorType> adaptors = new ArrayList<>();
		NIOAdaptorProperties nioProp = new NIOAdaptorProperties();
		nioProp.setMaxPort(maxPort);
		nioProp.setMinPort(minPort);
		AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, null);
		adaptors.add(adaptor);
		return this.addComputeNode(name, processors, adaptors, null, null, null, null, null, null);
	}

	/**
	 * Add an instance of compute node with a single processor and a GAT adaptor
	 * 
	 * @param name
	 *            Node name
	 * @param procName
	 *            Processor Name
	 * @param procCU
	 *            Processor Computing Units
	 * @param procArch
	 *            Processor Architecture
	 * @param procSpeed
	 *            Processor Speed
	 * @param procProp
	 *            Processor Property
	 * @param adaptorName
	 *            Adaptor Name
	 * @param bath
	 *            Maximum port number of the port range
	 * @param queues
	 *            Minimum port number of the port range
	 * @param interactive
	 *            Executor command
	 * @param brokerAdaptor
	 *            GAT broker adaptor
	 * @param user
	 *            User name
	 * @param memorySize
	 *            Memory size
	 * @param memoryType
	 *            Memory type
	 * @param storageSize
	 *            Storage size
	 * @param storageType
	 *            Storage type
	 * @param osType
	 *            Operating system type
	 * @param osDistribution
	 *            Operating system distribution
	 * @param osVersion
	 *            Operating system version
	 * @return
	 * @throws InvalidElementException
	 */
	public ComputeNodeType addComputeNode(String name, String procName, int procCU, String procArch, float procSpeed,
			String procType, float procMemSize, ProcessorPropertyType procProp, String adaptorName, boolean batch,
			List<String> queues, boolean interactive, String brokerAdaptor, String user, float memorySize,
			String memoryType, float storageSize, String storageType, String osType, String osDistribution,
			String osVersion) throws InvalidElementException {

		List<ProcessorType> processors = new ArrayList<>();
		ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
		processors.add(pr);
		MemoryType mem = createMemory(memorySize, memoryType);
		StorageType storage = createStorage(storageSize, storageType);
		OSType os = createOperatingSystem(osType, osDistribution, osVersion);
		List<AdaptorType> adaptors = new ArrayList<>();
		AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, batch, queues, interactive, brokerAdaptor, user);
		adaptors.add(adaptor);
		return this.addComputeNode(name, processors, adaptors, mem, storage, os, null, null, null);
	}

	/**
	 * Add the given Service @s to the resources file
	 *
	 * @param s
	 * @return
	 * @throws InvalidElementException
	 */
	public ServiceType addService(ServiceType s) throws InvalidElementException {
		validator.validateService(s);

		// If we reach this point means that the DataNode is valid
		this.resources.getSharedDiskOrDataNodeOrComputeNode().add(s);
		return s;
	}

	/**
	 * Adds a new Service with the given information and returns the instance of the
	 * new Service
	 *
	 * @param wsdl
	 * @param name
	 * @param namespace
	 * @param port
	 * @return
	 * @throws InvalidElementException
	 */
	public ServiceType addService(String wsdl, String name, String namespace, String port)
			throws InvalidElementException {
		return this.addService(wsdl, name, namespace, port, null);
	}

	/**
	 * Adds a new Service with the given information and returns the instance of the
	 * new Service
	 *
	 * @param wsdl
	 * @param name
	 * @param namespace
	 * @param port
	 * @param price
	 * @return
	 * @throws InvalidElementException
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
	 * Adds the given CloudProvider @cp to the resources file
	 *
	 * @param cp
	 * @return
	 * @throws InvalidElementException
	 */
	public CloudProviderType addCloudProvider(CloudProviderType cp) throws InvalidElementException {
		validator.validateCloudProvider(cp);

		// If we reach this point means that the DataNode is valid
		this.resources.getSharedDiskOrDataNodeOrComputeNode().add(cp);
		return cp;
	}

	/**
	 * Adds a new CloudProvider with the given information and returns the instance
	 * of the new CloudProvider
	 *
	 * @param name
	 * @param endpoint
	 * @param images
	 * @param instances
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new CloudProvider with the given information and returns the instance
	 * of the new CloudProvider
	 *
	 * @param name
	 * @param endpoint
	 * @param images
	 * @param instances
	 * @return
	 * @throws InvalidElementException
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
	 * Adds a new CloudProvider with the given information and returns the instance
	 * of the new CloudProvider
	 *
	 * @param name
	 * @param endpoint
	 * @param images
	 * @param instances
	 * @return
	 * @throws InvalidElementException
	 */
	public CloudProviderType addCloudProvider(String name, EndpointType endpoint, ImagesType images,
			List<InstanceTypeType> instances) throws InvalidElementException {

		InstanceTypesType instancesList = new InstanceTypesType();
		if (instances != null) {
			for (InstanceTypeType ins : instances) {
				instancesList.getInstanceType().add(ins);
			}
		}

		return this.addCloudProvider(name, endpoint, images, instancesList);
	}

	/**
	 * Adds a new CloudProvider with the given information and returns the instance
	 * of the new CloudProvider
	 *
	 * @param name
	 * @param endpoint
	 * @param images
	 * @param instances
	 * @return
	 * @throws InvalidElementException
	 */
	public CloudProviderType addCloudProvider(String name, EndpointType endpoint, List<ImageType> images,
			InstanceTypesType instances) throws InvalidElementException {

		ImagesType imagesList = new ImagesType();
		if (images != null) {
			for (ImageType im : images) {
				imagesList.getImage().add(im);
			}
		}

		return this.addCloudProvider(name, endpoint, imagesList, instances);
	}

	/**
	 * Adds a new CloudProvider with the given information and returns the instance
	 * of the new CloudProvider
	 *
	 * @param name
	 * @param server
	 * @param connector
	 * @param images
	 * @param instances
	 * @return
	 * @throws InvalidElementException
	 */
	public CloudProviderType addCloudProvider(String name, String server, String connectorJar, String connectorClass,
			List<ImageType> images, List<InstanceTypeType> instances) throws InvalidElementException {

		EndpointType endpoint = new EndpointType();
		JAXBElement<String> serverElement = new JAXBElement<String>(new QName("Server"), String.class, server);
		endpoint.getServerOrConnectorJarOrConnectorClass().add(serverElement);
		JAXBElement<String> connectorClassElement = new JAXBElement<String>(new QName("ConnectorClass"), String.class,
				connectorClass);
		JAXBElement<String> connectorJarElement = new JAXBElement<String>(new QName("ConnectorJar"), String.class,
				connectorJar);
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

	/*
	 * ************************************** SETTERS: HELPERS FOR SECOND LEVEL
	 * ELEMENTS
	 **************************************/

	/**
	 * Creates a Processor element
	 * 
	 * @param name
	 *            Processor Name
	 * @param cu
	 *            Processor Computing Units
	 * @param procArchitecture
	 *            Processor Architecture
	 * @param procSpeed
	 *            Porcessor Speed
	 * @param procProperty
	 *            Processor Property
	 * @return
	 */
	public static ProcessorType createProcessor(String name, int cu, String procArchitecture, float procSpeed,
			String type, float internalMemory, ProcessorPropertyType procProperties) {

		ProcessorType processor = new ProcessorType();
		processor.setName(name);
		JAXBElement<Integer> cuElement = new JAXBElement<Integer>(new QName("ComputingUnits"), Integer.class, cu);
		processor.getComputingUnitsOrArchitectureOrSpeed().add(cuElement);

		if (procArchitecture != null) {
			JAXBElement<String> archElement = new JAXBElement<String>(new QName("Architecture"), String.class,
					procArchitecture);
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
			JAXBElement<Float> memElement = new JAXBElement<Float>(new QName("InternalMemorySize"), Float.class,
					internalMemory);
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
	 * Creates a Processor Property element
	 * 
	 * @param key
	 *            Processor property key
	 * @param value
	 *            Processor property value
	 * @return
	 */
	public static ProcessorPropertyType createProcessorProperty(String key, String value) {
		ProcessorPropertyType prop = new ProcessorPropertyType();
		prop.setKey(key);
		prop.setValue(value);
		return prop;
	}

	/**
	 * Creates a Memory element
	 * 
	 * @param memorySize
	 *            Memory size
	 * @param type
	 *            Memory type
	 * @return
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
	 * Creates a Storage element
	 * 
	 * @param storageSize
	 *            Storage size
	 * @param type
	 *            Storage type
	 * @return
	 */
	public static StorageType createStorage(float storageSize, String type) {
		StorageType storage = new StorageType();
		storage.getSizeOrType().add(new Float(storageSize));
		if (type != null) {
			storage.getSizeOrType().add(type);
		}
		return storage;
	}

	/**
	 * Creates a Operating System element
	 * 
	 * @param osType
	 *            Operating System type (linux, windows, ...)
	 * @param osDistribution
	 *            Operating System Distribution (Ubuntu, Centos, ...)
	 * @param osVersion
	 *            Operating System version
	 * @return
	 */
	public static OSType createOperatingSystem(String osType, String osDistribution, String osVersion) {
		OSType os = new OSType();
		OSTypeType osTypeType = OSTypeType.fromValue(osType);
		JAXBElement<OSTypeType> typeElement = new JAXBElement<OSTypeType>(new QName("Type"), OSTypeType.class,
				osTypeType);
		os.getTypeOrDistributionOrVersion().add(typeElement);

		// Optional parameters
		if (osDistribution != null) {
			JAXBElement<String> distElement = new JAXBElement<String>(new QName("Distribution"), String.class,
					osDistribution);
			os.getTypeOrDistributionOrVersion().add(distElement);
		}
		if (osVersion != null) {
			JAXBElement<String> verElement = new JAXBElement<String>(new QName("Version"), String.class, osVersion);
			os.getTypeOrDistributionOrVersion().add(verElement);
		}

		return os;
	}

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

	public static ImageType createImage(String name, String adaptorName, int maxPort, int minPort, String executor,
			String user, String osType, String osDistribution, String osVersion) {

		ImageType image = new ImageType();
		image.setName(name);
		AdaptorsListType adaptorsList = new AdaptorsListType();
		NIOAdaptorProperties nioProp = new NIOAdaptorProperties();
		nioProp.setMaxPort(maxPort);
		nioProp.setMinPort(minPort);
		nioProp.setRemoteExecutionCommand(executor);
		AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, user);
		adaptorsList.getAdaptor().add(adaptor);
		image.getAdaptorsOrOperatingSystemOrSoftware().add(adaptorsList);
		OSType os = createOperatingSystem(osType, osDistribution, osVersion);
		image.getAdaptorsOrOperatingSystemOrSoftware().add(os);
		return image;

	}

	public static ImageType createImage(String name, String adaptorName, int maxPort, int minPort) {
		ImageType image = new ImageType();
		image.setName(name);
		AdaptorsListType adaptorsList = new AdaptorsListType();
		NIOAdaptorProperties nioProp = new NIOAdaptorProperties();
		nioProp.setMaxPort(maxPort);
		nioProp.setMinPort(minPort);
		AdaptorType adaptor = ResourcesFile.createAdaptor(adaptorName, false, null, true, nioProp, null);
		adaptorsList.getAdaptor().add(adaptor);
		image.getAdaptorsOrOperatingSystemOrSoftware().add(adaptorsList);
		return image;
	}

	public static InstanceTypeType createInstance(String name, String procName, int procCU, float memorySize,
			float storageSize) {
		return createInstance(name, procName, procCU, null, 0, null, 0, null, memorySize, null, storageSize, null);
	}

	public static InstanceTypeType createInstance(String name, String procName, int procCU, String procArch,
			float procSpeed, String procType, float procMemSize, ProcessorPropertyType procProp, float memorySize,
			String memoryType, float storageSize, String storageType) {

		InstanceTypeType instance = new InstanceTypeType();
		instance.setName(name);
		ProcessorType pr = createProcessor(procName, procCU, procArch, procSpeed, procType, procMemSize, procProp);
		instance.getProcessorOrMemoryOrStorage().add(pr);
		MemoryType mem = createMemory(memorySize, memoryType);
		instance.getProcessorOrMemoryOrStorage().add(mem);
		StorageType storage = createStorage(storageSize, storageType);
		instance.getProcessorOrMemoryOrStorage().add(storage);
		return instance;
	}

	public static EndpointType createEndpoint(String server, String connectorClass, String connectorJar, String port) {
		EndpointType endPoint = new EndpointType();

		JAXBElement<String> serverElement = new JAXBElement<String>(new QName("Server"), String.class, server);
		endPoint.getServerOrConnectorJarOrConnectorClass().add(serverElement);

		JAXBElement<String> connectorClassElement = new JAXBElement<String>(new QName("ConnectorClass"), String.class,
				connectorClass);
		endPoint.getServerOrConnectorJarOrConnectorClass().add(connectorClassElement);

		JAXBElement<String> connectorJarElement = new JAXBElement<String>(new QName("ConnectorJar"), String.class,
				connectorJar);
		endPoint.getServerOrConnectorJarOrConnectorClass().add(connectorJarElement);

		if (port != null) {
			JAXBElement<String> portElement = new JAXBElement<String>(new QName("Port"), String.class, port);
			endPoint.getServerOrConnectorJarOrConnectorClass().add(portElement);
		}
		return endPoint;
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
	public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
			NIOAdaptorProperties nioproperties, String user) {
		AdaptorType adaptor = new AdaptorType();
		adaptor.setName(name);

		JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(
				new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
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
	public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys, String gatproperties,
			String user) {
		AdaptorType adaptor = new AdaptorType();
		adaptor.setName(name);

		JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(
				new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
		adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

		JAXBElement<String> propertiesElement = new JAXBElement<String>(new QName("BrokerAdaptor"), String.class,
				gatproperties);
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
	public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
			ExternalAdaptorProperties externalproperties, String user) {
		AdaptorType adaptor = new AdaptorType();
		adaptor.setName(name);

		JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(
				new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
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
	 * Creates an instance of an Adaptor with the given information
	 *
	 * @param name
	 * @param subsys
	 * @param externalProperties
	 * @param user
	 * @return
	 */
	public static AdaptorType createAdaptor(String name, SubmissionSystemType subsys,
			List<PropertyAdaptorType> externalProperties, String user) {
		AdaptorType adaptor = new AdaptorType();
		adaptor.setName(name);

		JAXBElement<SubmissionSystemType> subsysElement = new JAXBElement<SubmissionSystemType>(
				new QName("SubmissionSystem"), SubmissionSystemType.class, subsys);
		adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor().add(subsysElement);

		ExternalAdaptorProperties externalproperties_list = new ExternalAdaptorProperties();
		if (externalProperties != null) {
			for (PropertyAdaptorType pa : externalProperties) {
				externalproperties_list.getProperty().add(pa);
			}
		}
		JAXBElement<ExternalAdaptorProperties> propertiesElement = new JAXBElement<ExternalAdaptorProperties>(
				new QName("Properties"), ExternalAdaptorProperties.class, externalproperties_list);
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
	 *            Adaptor's name
	 * @param batch
	 *            Flag to indicate if supports batch submission
	 * @param queues
	 *            Batch queues
	 * @param interactive
	 *            Flag to indicate if supports interactive submission
	 * @param nioproperties
	 *            NIO adaptor properties
	 * @param user
	 *            Username
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
	 *            Adaptor's name
	 * @param batch
	 *            Flag to indicate if supports batch submission
	 * @param queues
	 *            Batch queues
	 * @param interactive
	 *            Flag to indicate if supports interactive submission
	 * @param gatproperties
	 *            GAT broker adaptor
	 * @param user
	 *            Username
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
			subsys.getBatchOrInteractive().add(b);
		}
		if (interactive) {
			InteractiveType i = new InteractiveType();
			subsys.getBatchOrInteractive().add(i);
		}

		return createAdaptor(name, subsys, externalProperties, user);
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
			cp.getImages().getImage().add(image);
			return true;
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
			cp.getInstanceTypes().getInstanceType().add(instance);
			return true;
		} else {
			return false;
		}
	}

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

	/*
	 * ************************************** GETTERS: HELPERS FOR SECOND LEVEL
	 * ELEMENTS
	 **************************************/
	/**
	 * Returns the number of Compunting Units of a given processor @p
	 *
	 * @param p
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
	 * Returns the architecture of a given processor @p
	 *
	 * @param p
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
	 * Returns the speed of a given processor @p
	 *
	 * @param p
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
	 * Returns the type of a given processor @p
	 *
	 * @param p
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
	 * Returns the internal memory of a given processor @p
	 *
	 * @param p
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
	 * Returns the processor property of a given processor @p
	 *
	 * @param p
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

	/*
	 * ************************************** DELETERS: MAIN ELEMENTS
	 **************************************/
	/**
	 * Deletes the SharedDisk with name=@name Returns true if deletion is
	 * successfull, false otherwise
	 *
	 * @param name
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
	 * Deletes the DataNode with name=@name Returns true if deletion is successfull,
	 * false otherwise
	 *
	 * @param name
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
	 * Deletes the ComputeNode with name=@name Returns true if deletion is
	 * successfull, false otherwise
	 *
	 * @param name
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
	 * Deletes the Service with wsdl=@wsdl Returns true if deletion is successfull,
	 * false otherwise
	 *
	 * @param wsdl
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
	 * Deletes the SharedDisk with name=@name Returns true if deletion is
	 * successful, false otherwise
	 *
	 * @param name
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

	/*
	 * ************************************** PRIVATE METHODS FOR CALCULATION
	 **************************************/
	private float getStorageSize(StorageType storage) {
		List<Serializable> storageProps = storage.getSizeOrType();
		if (storageProps != null) {
			for (Serializable prop : storageProps) {
				if (prop instanceof Float) {
					return (float) prop;
				}
			}
		}
		return (float) -1.0;
	}

	private String getStorageType(StorageType storage) {
		List<Serializable> storageProps = storage.getSizeOrType();
		if (storageProps != null) {
			for (Serializable prop : storageProps) {
				if (prop instanceof String) {
					return (String) prop;
				}
			}
		}

		return null;
	}

}
