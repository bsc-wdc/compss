package es.bsc.compss.types.resources;

import es.bsc.compss.types.resources.exceptions.InvalidElementException;
import es.bsc.compss.types.resources.exceptions.ResourcesFileValidationException;
import es.bsc.compss.types.resources.jaxb.AdaptorType;
import es.bsc.compss.types.resources.jaxb.AdaptorsListType;
import es.bsc.compss.types.resources.jaxb.AttachedDiskType;
import es.bsc.compss.types.resources.jaxb.AttachedDisksListType;
import es.bsc.compss.types.resources.jaxb.BatchType;
import es.bsc.compss.types.resources.jaxb.CloudProviderType;
import es.bsc.compss.types.resources.jaxb.ComputeNodeType;
import es.bsc.compss.types.resources.jaxb.DataNodeType;
import es.bsc.compss.types.resources.jaxb.EndpointType;
import es.bsc.compss.types.resources.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ImageType;
import es.bsc.compss.types.resources.jaxb.ImagesType;
import es.bsc.compss.types.resources.jaxb.InstanceTypeType;
import es.bsc.compss.types.resources.jaxb.InstanceTypesType;
import es.bsc.compss.types.resources.jaxb.InteractiveType;
import es.bsc.compss.types.resources.jaxb.MemoryType;
import es.bsc.compss.types.resources.jaxb.NIOAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.OSType;
import es.bsc.compss.types.resources.jaxb.PriceType;
import es.bsc.compss.types.resources.jaxb.ProcessorType;
import es.bsc.compss.types.resources.jaxb.ServiceType;
import es.bsc.compss.types.resources.jaxb.SharedDiskType;
import es.bsc.compss.types.resources.jaxb.SoftwareListType;
import es.bsc.compss.types.resources.jaxb.StorageType;
import es.bsc.compss.types.resources.jaxb.SubmissionSystemType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;

import org.apache.logging.log4j.Logger;


/**
 *
 * Custom XML Validation for COMPSs
 *
 */
public class Validator {

    private ResourcesFile rf;

    private Logger logger;


    /**
     * Validator instantiation for ResourcesFile rf
     *
     * @param rf
     */
    public Validator(ResourcesFile rf, Logger logger) {
        this.logger = logger;
        this.rf = rf;
    }

    /**
     * Validates the content of the given ResourcesFile object The content is correct if no exception is raised
     *
     * @throws JAXBException
     * @throws ResourcesFileValidationException
     */
    public void validate() throws ResourcesFileValidationException {
        logger.info("Validating <ResourcesList> tag");
        // Check inner elements
        List<Object> objList = rf.getResources().getSharedDiskOrDataNodeOrComputeNode();
        if (objList != null) {
            try {
                boolean minimumUsableElementFound = false;
                for (Object obj : objList) {
                    if (obj instanceof SharedDiskType) {
                        validateSharedDisk(((SharedDiskType) obj));
                    } else if (obj instanceof DataNodeType) {
                        validateDataNode(((DataNodeType) obj));
                    } else if (obj instanceof ComputeNodeType) {
                        minimumUsableElementFound = true;
                        validateComputeNode(((ComputeNodeType) obj));
                    } else if (obj instanceof ServiceType) {
                        minimumUsableElementFound = true;
                        validateService(((ServiceType) obj));
                    } else if (obj instanceof CloudProviderType) {
                        minimumUsableElementFound = true;
                        validateCloudProvider(((CloudProviderType) obj));
                    } else {
                        throw new InvalidElementException("Resources", "Attribute" + obj.getClass(), "Incorrect attribute");
                    }
                }

                if (!minimumUsableElementFound) {
                    throw new InvalidElementException("Resources", "", "Any computational (computeNode, service or cloud) resources found");
                }
            } catch (InvalidElementException iee) {
                throw ((ResourcesFileValidationException) iee);
            }
        } else {
            // If resources is empty, raise exception
            throw new ResourcesFileValidationException("Empty resources file");
        }
        logger.info("End validation");
    }

    /*
     * ********************************************** 
     * VALIDATION OF MAIN ELEMENTS
     **********************************************/
    /**
     * Validates the given SharedDiskType @sd with the current ResourcesFileType The content is correct if no exception
     * is raised
     *
     * @param sd
     * @throws InvalidElementException
     */
    public void validateSharedDisk(SharedDiskType sd) throws InvalidElementException {
        // Check that name isn't used
        int num = 0;
        for (String s : rf.getSharedDisks_names()) {
            if (s.equals(sd.getName())) {
                num = num + 1;
            }
        }
        if (num > 1) {
            // Shared disk already exists
            throw new InvalidElementException("SharedDisk", sd.getName(), "Name already in use");
        }

        // If storage exists (it is not mandatory), check if it is valid
        if (sd.getStorage() != null) {
            validateStorage(sd.getStorage());
        }
    }

    /**
     * Validates the given DataNodeType @dn with the current ResourcesFileType The content is correct if no exception is
     * raised
     *
     * @param dn
     * @throws InvalidElementException
     */
    public void validateDataNode(DataNodeType dn) throws InvalidElementException {
        // Check that name isn't used
        int num = 0;
        for (String d : rf.getDataNodes_names()) {
            if (d.equals(dn.getName())) {
                num = num + 1;
            }
        }
        if (num > 1) {
            // DataNode already exists
            throw new InvalidElementException("DataNode", dn.getName(), "Name already in use");
        }

        // Check inner elements
        List<JAXBElement<?>> innerElements = dn.getHostOrPathOrAdaptors();
        if (innerElements != null) {
            boolean hostTagFound = false;
            boolean pathTagFound = false;
            boolean adaptorsTagFound = false;
            boolean storageTagFound = false;
            boolean sharedDisksTagFound = false;
            for (JAXBElement<?> obj : innerElements) {
                if (obj.getName().equals(new QName("Host"))) {
                    if (hostTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(),
                                "Appears more than once");
                    } else {
                        hostTagFound = true;
                    }
                } else if (obj.getName().equals(new QName("Path"))) {
                    if (pathTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(),
                                "Appears more than once");
                    } else {
                        pathTagFound = true;
                    }
                } else if (obj.getName().equals(new QName("Adaptors"))) {
                    if (adaptorsTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(),
                                "Appears more than once");
                    } else {
                        adaptorsTagFound = true;
                        validateAdaptors(((AdaptorsListType) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("Storage"))) {
                    if (storageTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(),
                                "Appears more than once");
                    } else {
                        storageTagFound = true;
                        validateStorage(((StorageType) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("SharedDisks"))) {
                    if (sharedDisksTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(),
                                "Appears more than once");
                    } else {
                        sharedDisksTagFound = true;
                        validateAttachedDisksList(((AttachedDisksListType) obj.getValue()));
                    }
                } else {
                    throw new InvalidElementException("DataNode " + dn.getName(), "Attribute " + obj.getName(), "Incorrect attribute");
                }
            }

            // Check minimum appearences
            if (!hostTagFound) {
                throw new InvalidElementException("DataNode " + dn.getName(), "Attribute Host", "Doesn't appear");
            }
            if (!pathTagFound) {
                throw new InvalidElementException("DataNode " + dn.getName(), "Attribute Path", "Doesn't appear");
            }
            if (!adaptorsTagFound) {
                throw new InvalidElementException("DataNode " + dn.getName(), "Attribute Adaptors", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("DataNode " + dn.getName(), "", "Content is empty");
        }
    }

    /**
     * Validates the given ComputeNodeType @cn with the current ResourcesFileType The content is correct if no exception
     * is raised
     *
     * @param cn
     * @throws InvalidElementException
     */
    public void validateComputeNode(ComputeNodeType cn) throws InvalidElementException {
        // Check that name isn't used
        int num = 0;
        for (String c : rf.getComputeNodes_names()) {
            if (c.equals(cn.getName())) {
                num = num + 1;
            }
        }
        if (num > 1) {
            // ComputeNode already exists
            throw new InvalidElementException("ComputeNode", cn.getName(), "Name already in use");
        }

        // Check inner elements
        List<Object> innerElements = cn.getProcessorOrAdaptorsOrMemory();
        if (innerElements != null) {
            List<String> processorNames = new ArrayList<String>();
            boolean processorTagFound = false;
            boolean adaptorsTagFound = false;
            boolean memoryTagFound = false;
            boolean storageTagFound = false;
            boolean osTagFound = false;
            boolean softwareTagFound = false;
            boolean sharedDisksTagFound = false;
            boolean priceTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof ProcessorType) {
                    ProcessorType p = (ProcessorType) obj;
                    if (processorNames.contains(p.getName())) {
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute Processor" + p.getName(),
                                "Appears more than once");
                    } else {
                        processorTagFound = true;
                        processorNames.add(p.getName());
                        validateProcessor(p);
                    }
                } else if (obj instanceof AdaptorsListType) {
                    if (adaptorsTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        adaptorsTagFound = true;
                        validateAdaptors(((AdaptorsListType) obj));
                    }
                } else if (obj instanceof MemoryType) {
                    if (memoryTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        memoryTagFound = true;
                        validateMemory(((MemoryType) obj));
                    }
                } else if (obj instanceof StorageType) {
                    if (storageTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        storageTagFound = true;
                        validateStorage(((StorageType) obj));
                    }
                } else if (obj instanceof OSType) {
                    if (osTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        osTagFound = true;
                        validateOS(((OSType) obj));
                    }
                } else if (obj instanceof SoftwareListType) {
                    if (softwareTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        softwareTagFound = true;
                        validateSoftwareList(((SoftwareListType) obj));
                    }
                } else if (obj instanceof AttachedDisksListType) {
                    if (sharedDisksTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        sharedDisksTagFound = true;
                        validateAttachedDisksList(((AttachedDisksListType) obj));
                    }
                } else if (obj instanceof PriceType) {
                    if (priceTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(),
                                "Appears more than once");
                    } else {
                        priceTagFound = true;
                        validatePrice(((PriceType) obj));
                    }
                } else {
                    throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!processorTagFound) {
                throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute Processor", "Doesn't appear");
            }
            if (!adaptorsTagFound) {
                throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute Adaptors", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("ComputeNode " + cn.getName(), "", "Content is empty");
        }
    }

    /**
     * Validates the given ServiceType @s with the current ResourcesFileType The content is correct if no exception is
     * raised
     *
     * @param s
     * @throws InvalidElementException
     */
    public void validateService(ServiceType s) throws InvalidElementException {
        // Check that name isn't used
        int num = 0;
        for (String s2 : rf.getServices_wsdls()) {
            if (s2.equals(s.getWsdl())) {
                num = num + 1;
            }
        }
        if (num > 1) {
            // Service already exists
            throw new InvalidElementException("Service", s.getWsdl(), "WSDL already in use");
        }

        // Check that mandatory tags are present
        if ((s.getName() == null) || (s.getName().isEmpty())) {
            throw new InvalidElementException("Service " + s.getWsdl(), "Attribute Name", "Doesn't appear");
        }
        if ((s.getNamespace() == null) || (s.getNamespace().isEmpty())) {
            throw new InvalidElementException("Service " + s.getWsdl(), "Attribute Namespace", "Doesn't appear");
        }
        if ((s.getPort() == null) || (s.getPort().isEmpty())) {
            throw new InvalidElementException("Service " + s.getWsdl(), "Attribute Port", "Doesn't appear");
        }

        // Check inner elements
        if (s.getPrice() != null) {
            validatePrice(s.getPrice());
        }
    }

    /**
     * Validates the given CloudProviderType @cp with the current ResourcesFileType The content is correct if no
     * exception is raised
     *
     * @param cp
     * @throws InvalidElementException
     */
    public void validateCloudProvider(CloudProviderType cp) throws InvalidElementException {
        // Check that name isn't used
        int num = 0;
        for (String cp2 : rf.getCloudProviders_names()) {
            if (cp2.equals(cp.getName())) {
                num = num + 1;
            }
        }
        if (num > 1) {
            // CloudProvider already exists
            throw new InvalidElementException("CloudProvider", cp.getName(), "Name already in use");
        }

        // Check inner elements
        if (cp.getEndpoint() == null) {
            throw new InvalidElementException("CloudProvider " + cp.getName(), "Attribute Endpoint", "Doesn't appear");
        } else {
            validateEndpoint(cp.getEndpoint());
        }
        if (cp.getImages() == null) {
            throw new InvalidElementException("CloudProvider " + cp.getName(), "Attribute Images", "Doesn't appear");
        } else {
            validateImages(cp.getImages());
        }
        if (cp.getInstanceTypes() == null) {
            throw new InvalidElementException("CloudProvider " + cp.getName(), "Attribute InstanceTypes", "Doesn't appear");
        } else {
            validateInstanceTypes(cp.getInstanceTypes());
        }
    }

    /*
     * ********************************************** 
     * HELPERS FOR VALIDATION (PRIVATE METHODS)
     **********************************************/
    private void validateStorage(StorageType s) throws InvalidElementException {
        // Validate inner elements
        List<Serializable> innerElements = s.getSizeOrType();
        if (innerElements != null) {
            boolean sizeTagFound = false;
            boolean typeTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof Float) {
                    if (sizeTagFound) {
                        throw new InvalidElementException("Storage", "Attribute Size", "Appears more than once");
                    } else {
                        sizeTagFound = true;
                        Float val = (Float) obj;
                        if (val <= 0.0) {
                            throw new InvalidElementException("Storage", "Attribute Size", "Must be greater than 0");
                        }
                    }
                } else if (obj instanceof String) {
                    if (typeTagFound) {
                        throw new InvalidElementException("Storage", "Attribute Type", "Appears more than once");
                    } else {
                        typeTagFound = true;
                    }
                } else {
                    throw new InvalidElementException("Storage", "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!sizeTagFound) {
                throw new InvalidElementException("Storage ", "Attribute Size", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Storage", "", "Content is empty");
        }
    }

    private void validateAdaptors(AdaptorsListType adaptors) throws InvalidElementException {
        // Validate inner elements
        if (adaptors != null) {
            List<AdaptorType> adaptors_list = adaptors.getAdaptor();
            if (adaptors_list != null) {
                for (AdaptorType adaptor : adaptors_list) {
                    validateAdaptor(adaptor);
                }
            } else {
                // Empty inner elements
                throw new InvalidElementException("Adaptors", "", "Content is empty");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Adaptors", "", "Tag not present");
        }
    }

    private void validateAdaptor(AdaptorType adaptor) throws InvalidElementException {
        // Validate inner elements
        List<JAXBElement<?>> innerElements = adaptor.getSubmissionSystemOrPortsOrBrokerAdaptor();
        if (innerElements != null) {
            boolean subsysTagFound = false;
            boolean portsTagFound = false;
            boolean brokerAdaptorTagFound = false;
            boolean propertiesTagFound = false;
            boolean userTagFound = false;
            for (JAXBElement<?> obj : innerElements) {
                if (obj.getName().equals(new QName("SubmissionSystem"))) {
                    if (subsysTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(), "Appears more than once");
                    } else {
                        subsysTagFound = true;
                        validateSubmissionSystem(((SubmissionSystemType) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("Ports"))) {
                    if (portsTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(), "Appears more than once");
                    } else if (brokerAdaptorTagFound || propertiesTagFound) {
                        // Cannot define multiple adaptor properties
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(),
                                "An adaptor property (GAT or External) was already defined");
                    } else {
                        portsTagFound = true;
                        validateNIOAdaptorProperties(((NIOAdaptorProperties) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("BrokerAdaptor"))) {
                    if (brokerAdaptorTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(), "Appears more than once");
                    } else if (portsTagFound || propertiesTagFound) {
                        // Cannot define multiple adaptor properties
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(),
                                "An adaptor property (NIO or External) was already defined");
                    } else {
                        brokerAdaptorTagFound = true;
                        validateGATAdaptorProperties(((String) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("Properties"))) {
                    if (propertiesTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(), "Appears more than once");
                    } else if (portsTagFound || brokerAdaptorTagFound) {
                        // Cannot define multiple adaptor properties
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(),
                                "An adaptor property (NIO or GAT) was already defined");
                    } else {
                        propertiesTagFound = true;
                        validateExternalAdaptorProperties(((ExternalAdaptorProperties) obj.getValue()));
                    }
                } else if (obj.getName().equals(new QName("User"))) {
                    if (userTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Adaptor", "Attribute " + obj.getName(), "Appears more than once");
                    } else {
                        userTagFound = true;
                    }
                } else {
                    throw new InvalidElementException("Adaptor " + adaptor.getName(), "Attribute" + obj.getName(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!subsysTagFound) {
                throw new InvalidElementException("Adaptor " + adaptor.getName(), "Attribute SubmissionSystem", "Doesn't appear");
            }
            if (!portsTagFound && !brokerAdaptorTagFound && !propertiesTagFound) {
                throw new InvalidElementException("Adaptor " + adaptor.getName(), "Attribute Adaptor properties (NIO, GAT or External)",
                        "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Adaptor", "", "Content is empty");
        }
    }

    private void validateSubmissionSystem(SubmissionSystemType subSys) throws InvalidElementException {
        List<Object> innerElements = subSys.getBatchOrInteractive();
        if (innerElements != null) {
            boolean batchTagFound = false;
            boolean interactiveTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof BatchType) {
                    if (batchTagFound) {
                        throw new InvalidElementException("SubmissionSystem", "Attribute Batch", "Appears more than once");
                    } else {
                        batchTagFound = true;
                        validateBatch(((BatchType) obj));
                    }
                } else if (obj instanceof InteractiveType) {
                    if (interactiveTagFound) {
                        throw new InvalidElementException("SubmissionSystem", "Attribute Interactive", "Appears more than once");
                    } else {
                        interactiveTagFound = true;
                        validateInteractive(((InteractiveType) obj));
                    }
                } else {
                    throw new InvalidElementException("SubmissionSystem", "Attribute " + obj.getClass(), "Invalid type");
                }
            }
            // Check minimum
            if (!batchTagFound && !interactiveTagFound) {
                throw new InvalidElementException("SubmissionSystem", "Interactive/Batch", "At least 1 submission system must be defined");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("SubmissionSystem", "", "Content is empty");
        }
    }

    private void validateBatch(BatchType batch) throws InvalidElementException {
        // The queue information is only a string name selected by the user
        // and thus it is always valid. Nothing to check
    }

    private void validateInteractive(InteractiveType interactive) throws InvalidElementException {
        // The interactive type is empty and thus, always valid
    }

    private void validateNIOAdaptorProperties(NIOAdaptorProperties props) throws InvalidElementException {
        if (props.getMaxPort() < 0) {
            throw new InvalidElementException("Properties of NIO Adaptor", "Attribute MaxPort", "Invalid value");
        }
        if (props.getMinPort() < 0) {
            throw new InvalidElementException("Properties of NIO Adaptor", "Attribute MinPort", "Invalid value");
        }
        if (props.getMaxPort() <= props.getMinPort()) {
            throw new InvalidElementException("Properties of NIO Adaptor", "Attribute MinPort-MaxPort", "Invalid range");
        }
    }

    private void validateGATAdaptorProperties(String broker) throws InvalidElementException {
        if ((broker == null) || (broker.isEmpty())) {
            throw new InvalidElementException("GATAdaptor", "Attribute BrokerAdaptor", "Doesn't appear");
        }
    }

    private void validateExternalAdaptorProperties(ExternalAdaptorProperties props) throws InvalidElementException {
        // The list of properties is a user defined List< (Name,Value) >
        // Since the list is not empty (XSD validation) and the user can add any property
        // we consider that it is always valid.
    }

    private void validateAttachedDisksList(AttachedDisksListType disks) throws InvalidElementException {
        // Validate inner elements
        List<AttachedDiskType> innerElements = disks.getAttachedDisk();
        if (innerElements != null) {
            for (AttachedDiskType disk : innerElements) {
                validateAttachedDisk(disk);
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("AttachedDisksList", "", "Content is empty");
        }
    }

    private void validateAttachedDisk(AttachedDiskType disk) throws InvalidElementException {
        if (disk.getName() == null || disk.getName().isEmpty()) {
            throw new InvalidElementException("AttachedDisk ", "Attribute Name", "Doesn't appear");
        }
        if (rf.getSharedDisk(disk.getName()) == null) {
            // The disk has not been defined
            throw new InvalidElementException("AttachedDisk" + disk.getName(), "", "Not defined as SharedDisk");
        }
        if (disk.getMountPoint() == null || disk.getMountPoint().isEmpty()) {
            throw new InvalidElementException("AttachedDisk" + disk.getName(), "Attribute MountPoint", "Doesn't appear");
        }
    }

    private void validateProcessor(ProcessorType processor) throws InvalidElementException {
        // Names are validated in the parent node

        // Validate inner elements
    	List<JAXBElement<?>> innerElements = processor.getComputingUnitsOrArchitectureOrSpeed();
        if (innerElements != null) {
            boolean cuTagFound = false;
            boolean ArchitectureTagFound = false;
            boolean speedTagFound = false;
            boolean typeTagFound = false;
            boolean memTagFound = false;
            boolean processorPropertyTagFound = false;
            for (JAXBElement<?> obj : innerElements) {
                if (obj.getName().equals(new QName("ComputingUnits"))) {
                    if (cuTagFound) {
                        throw new InvalidElementException("Processor", "Attribute ComputingUnits", "Appears more than once");
                    } else {
                        cuTagFound = true;
                        int val = (Integer) obj.getValue();
                        if (val <= 0) {
                            throw new InvalidElementException("Processor", "Attribute ComputingUnits", "Must be greater than 0");
                        }
                    }
                } else if (obj.getName().equals(new QName("Architecture"))) {
                    if (ArchitectureTagFound) {
                        throw new InvalidElementException("Processor", "Attribute Architecture", "Appears more than once");
                    } else {
                        ArchitectureTagFound = true;
                    }
                } else if (obj.getName().equals(new QName("Speed"))) {
                    if (speedTagFound) {
                        throw new InvalidElementException("Processor", "Attribute Speed", "Appears more than once");
                    } else {
                        speedTagFound = true;
                        float val = (Float) obj.getValue();
                        if (val <= 0.0) {
                            throw new InvalidElementException("Processor", "Attribute Speed", "Must be greater than 0");
                        }
                    }
                } else if (obj.getName().equals(new QName("Type"))) {
                    if (typeTagFound) {
                        throw new InvalidElementException("Processor", "Attribute Type", "Appears more than once");
                    } else {
                        typeTagFound = true;
                    }
                } else if (obj.getName().equals(new QName("InternalMemorySize"))) {
                    if (memTagFound) {
                        throw new InvalidElementException("Processor", "Attribute InternalMemorySize", "Appears more than once");
                    } else {
                        memTagFound = true;
                        float val = (Float) obj.getValue();
                        if (val <= 0.0) {
                            throw new InvalidElementException("Processor", "Attribute InternalMemorySize", "Must be greater than 0");
                        }
                    }
                } else if (obj.getName().equals(new QName("ProcessorProperty"))) {
                    if (processorPropertyTagFound) {
                        throw new InvalidElementException("Processor", "Attribute ProcessorProperty", "Appears more than once");
                    } else {
                        processorPropertyTagFound = true;
                    }
                } else {
                    throw new InvalidElementException("Processor", "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!cuTagFound) {
                throw new InvalidElementException("Processor", "Attribute ComputingUnits", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Processor", "", "Content is empty");
        }
    }

    private void validateMemory(MemoryType memory) throws InvalidElementException {
        // Validate inner elements
        List<Serializable> innerElements = memory.getSizeOrType();
        if (innerElements != null) {
            boolean sizeTagFound = false;
            boolean typeTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof Float) {
                    if (sizeTagFound) {
                        throw new InvalidElementException("Memory", "Attribute Size", "Appears more than once");
                    } else {
                        sizeTagFound = true;
                        Float val = (Float) obj;
                        if (val <= 0.0) {
                            throw new InvalidElementException("Memory", "Attribute Size", "Must be greater than 0");
                        }
                    }
                } else if (obj instanceof String) {
                    if (typeTagFound) {
                        throw new InvalidElementException("Memory", "Attribute Type", "Appears more than once");
                    } else {
                        typeTagFound = true;
                    }
                } else {
                    throw new InvalidElementException("Memory", "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!sizeTagFound) {
                throw new InvalidElementException("Memory ", "Attribute Size", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Memory", "", "Content is empty");
        }
    }

    private void validateOS(OSType os) throws InvalidElementException {
        // The XSD ensures the OS Type and there is nothing more to validate
    }

    private void validateSoftwareList(SoftwareListType software) throws InvalidElementException {
        // The Software is a List of application names. The XSD ensure they are not empty
        // The names are user defined so there is nothig more to validate
    }

    private void validatePrice(PriceType price) throws InvalidElementException {
        if (price.getTimeUnit() <= 0) {
            throw new InvalidElementException("Price", "Attribute TimeUnit", "Invalid value");
        }
        if (price.getPricePerUnit() < 0) {
            throw new InvalidElementException("Price", "Attribute PricePerUnit", "Invalid value");
        }
    }

    private void validateEndpoint(EndpointType endpoint) throws InvalidElementException {
        // The endpoint has a Server / Connector / Port non-empty due to XSD
        // All the fields are user defined Strings and thus, there is nothing to validate
    }

    private void validateImages(ImagesType images) throws InvalidElementException {
        List<String> imageNames = new ArrayList<String>();

        List<ImageType> images_list = images.getImage();
        if (images_list != null) {
            for (ImageType im : images_list) {
                if (imageNames.contains(im.getName())) {
                    throw new InvalidElementException("Images", "Attribute Image " + im.getName(), "Name already used");
                } else {
                    imageNames.add(im.getName());
                    validateImage(im);
                }
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Images", "", "Content is empty");
        }
    }

    private void validateImage(ImageType image) throws InvalidElementException {
        // Validate inner elements
        List<Object> innerElements = image.getAdaptorsOrOperatingSystemOrSoftware();
        if (innerElements != null) {
            boolean adaptorsTagFound = false;
            boolean OSTagFound = false;
            boolean softwareTagFound = false;
            boolean sdTagFound = false;
            boolean priceTagFound = false;
            boolean creationTimeTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof AdaptorsListType) {
                    if (adaptorsTagFound) {
                        throw new InvalidElementException("Image", "Attribute Adaptors", "Appears more than once");
                    } else {
                        adaptorsTagFound = true;
                        validateAdaptors(((AdaptorsListType) obj));
                    }
                } else if (obj instanceof OSType) {
                    if (OSTagFound) {
                        throw new InvalidElementException("Image", "Attribute OperatingSystem", "Appears more than once");
                    } else {
                        OSTagFound = true;
                        validateOS(((OSType) obj));
                    }
                } else if (obj instanceof SoftwareListType) {
                    if (softwareTagFound) {
                        throw new InvalidElementException("Image", "Attribute Software", "Appears more than once");
                    } else {
                        softwareTagFound = true;
                        validateSoftwareList(((SoftwareListType) obj));
                    }
                } else if (obj instanceof AttachedDisksListType) {
                    if (sdTagFound) {
                        throw new InvalidElementException("Image", "Attribute AttachedDisks", "Appears more than once");
                    } else {
                        sdTagFound = true;
                        this.validateAttachedDisksList(((AttachedDisksListType) obj));
                    }
                } else if (obj instanceof PriceType) {
                    if (priceTagFound) {
                        throw new InvalidElementException("Image", "Attribute Price", "Appears more than once");
                    } else {
                        priceTagFound = true;
                        validatePrice(((PriceType) obj));
                    }
                } else if (obj instanceof java.lang.Integer) { // Creation time (int)
                    if (creationTimeTagFound) {
                        throw new InvalidElementException("Image", "Attribute CreationTime", "Appears more than once");
                    } else {
                        creationTimeTagFound = true;
                        // Nothing to validate since it is an integer
                    }
                } else {
                    throw new InvalidElementException("Image", "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!adaptorsTagFound) {
                throw new InvalidElementException("Image ", "Attribute Adaptors", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Image", "", "Content is empty");
        }
    }

    private void validateInstanceTypes(InstanceTypesType instances) throws InvalidElementException {
        List<String> instanceNames = new ArrayList<String>();

        List<InstanceTypeType> instances_list = instances.getInstanceType();
        if (instances_list != null) {
            for (InstanceTypeType i : instances.getInstanceType()) {
                if (instanceNames.contains(i.getName())) {
                    throw new InvalidElementException("Instances", "Attribute Instance " + i.getName(), "Name already used");
                } else {
                    instanceNames.add(i.getName());
                    validateInstance(i);
                }
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("InstanceTypes", "", "Content is empty");
        }
    }

    private void validateInstance(InstanceTypeType instance) throws InvalidElementException {
        // Validate inner elements
        List<Object> innerElements = instance.getProcessorOrMemoryOrStorage();
        if (innerElements != null) {
            List<String> processorNames = new ArrayList<String>();
            boolean processorTagFound = false;
            boolean memoryTagFound = false;
            boolean storageTagFound = false;
            boolean priceTagFound = false;
            for (Object obj : innerElements) {
                if (obj instanceof ProcessorType) {
                    processorTagFound = true;
                    ProcessorType p = (ProcessorType) obj;
                    if (processorNames.contains(p.getName())) {
                        throw new InvalidElementException("Instance", "Attribute Processor " + p.getName(), "Appears more than once");
                    } else {
                        processorNames.add(p.getName());
                        validateProcessor(((ProcessorType) obj));
                    }
                } else if (obj instanceof MemoryType) {
                    if (memoryTagFound) {
                        throw new InvalidElementException("Instance", "Attribute Memory", "Appears more than once");
                    } else {
                        memoryTagFound = true;
                        validateMemory(((MemoryType) obj));
                    }
                } else if (obj instanceof StorageType) {
                    if (storageTagFound) {
                        throw new InvalidElementException("Instance", "Attribute Storage", "Appears more than once");
                    } else {
                        storageTagFound = true;
                        validateStorage(((StorageType) obj));
                    }
                } else if (obj instanceof PriceType) {
                    if (priceTagFound) {
                        throw new InvalidElementException("Instance", "Attribute Price", "Appears more than once");
                    } else {
                        priceTagFound = true;
                        validatePrice(((PriceType) obj));
                    }
                } else {
                    throw new InvalidElementException("Instance", "Attribute " + obj.getClass(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!processorTagFound) {
                throw new InvalidElementException("Insntace", "Attribute Processor", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("InstanceType", "", "Content is empty");
        }
    }

}
