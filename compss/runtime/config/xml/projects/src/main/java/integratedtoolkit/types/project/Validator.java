package integratedtoolkit.types.project;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.apache.logging.log4j.Logger;

import integratedtoolkit.types.project.exceptions.ProjectFileValidationException;
import integratedtoolkit.types.project.exceptions.InvalidElementException;
import integratedtoolkit.types.project.jaxb.*;


/**
 * 
 * Custom XML Validation for COMPSs
 * 
 */
public class Validator {

    private ProjectFile pf;

    // Logger
    private Logger logger;


    /**
     * Validator instantiation for ProjectFile pf
     * 
     * @param pf
     */
    public Validator(ProjectFile pf, Logger logger) {
        this.logger = logger;
        this.pf = pf;
    }

    /**
     * Validates the content of the given ResourcesFile object The content is correct if no exception is raised
     * 
     * @throws JAXBException
     * @throws ResourcesFileValidationException
     */
    public void validate() throws ProjectFileValidationException {
        logger.info("Validating <Project> tag");
        // Check inner elements
        List<Object> objList = pf.getProject().getMasterNodeOrComputeNodeOrDataNode();
        if (objList != null) {
            try {
                boolean masterNodeTagFound = false;
                boolean minimumUsableElementFound = false;
                for (Object obj : pf.getProject().getMasterNodeOrComputeNodeOrDataNode()) {
                    if (obj instanceof MasterNodeType) {
                        if (masterNodeTagFound) {
                            throw new InvalidElementException("Project", "Attribute MasterNode", "Appears more than once");
                        } else {
                            masterNodeTagFound = true;
                            validateMasterNode(((MasterNodeType) obj));
                        }
                    } else if (obj instanceof ComputeNodeType) {
                        minimumUsableElementFound = true;
                        validateComputeNode(((ComputeNodeType) obj));
                    } else if (obj instanceof DataNodeType) {
                        validateDataNode(((DataNodeType) obj));
                    } else if (obj instanceof ServiceType) {
                        minimumUsableElementFound = true;
                        validateService(((ServiceType) obj));
                    } else if (obj instanceof CloudType) {
                        minimumUsableElementFound = true;
                        validateCloud(((CloudType) obj));
                    } else {
                        throw new InvalidElementException("Project", "Attribute" + obj.getClass(), "Incorrect attribute");
                    }
                }

                if (!masterNodeTagFound) {
                    throw new InvalidElementException("Project", "Attribute MasterNode", "Doesn't appear");
                }
                if (!minimumUsableElementFound) {
                    throw new InvalidElementException("Resources", "", "Any computational (computeNode, service or cloud) resources found");
                }
            } catch (InvalidElementException iee) {
                throw ((ProjectFileValidationException) iee);
            }
        } else {
            // If resources is empty, raise exception
            throw new ProjectFileValidationException("Empty project file");
        }
        logger.info("Validation finished");
    }

    /*
     * ********************************************** 
     * VALIDATION OF MAIN ELEMENTS
     **********************************************/
    /**
     * Validates the MasterNode
     * 
     * @param mn
     * @throws InvalidElementException
     */
    public void validateMasterNode(MasterNodeType mn) throws InvalidElementException {
        if (mn != null) {
            List<Object> innerElements = mn.getSharedDisksOrPrice();
            if (innerElements != null) {
                boolean sharedDisksTagFound = false;
                boolean priceTagFound = false;
                for (Object obj : innerElements) {
                    if (obj instanceof AttachedDisksListType) {
                        if (sharedDisksTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("MasterNode", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            sharedDisksTagFound = true;
                            validateAttachedDisksList(((AttachedDisksListType) obj));
                        }
                    } else if (obj instanceof PriceType) {
                        if (priceTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("MasterNode", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            priceTagFound = true;
                            validatePrice(((PriceType) obj));
                        }
                    } else {
                        throw new InvalidElementException("MasterNode", "Attribute " + obj.getClass(), "Incorrect attribute");
                    }
                }
            } else {
                // MasterNode has no mandatory fields --> can be empty
                // Don't raise exception
            }
        } else {
            // The MasterNode itself is null, raise exception
            throw new InvalidElementException("Project", "Attribute MasterNode", "is null");
        }
    }

    /**
     * Validates a ComputeNode
     * 
     * @param cn
     * @throws InvalidElementException
     */
    public void validateComputeNode(ComputeNodeType cn) throws InvalidElementException {
        if (cn != null) {
            List<JAXBElement<?>> innerElements = cn.getInstallDirOrWorkingDirOrUser();
            if (innerElements != null) {
                boolean installDirTagFound = false;
                boolean workingDirTagFound = false;
                boolean userTagFound = false;
                boolean appTagFound = false;
                boolean lotTagFound = false;
                boolean adaptorsTagFound = false;
                for (JAXBElement<?> element : innerElements) {
                    if (element.getName().equals(new QName("InstallDir"))) {
                        if (installDirTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            installDirTagFound = true;
                            validateInstallDir(((String) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("WorkingDir"))) {
                        if (workingDirTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            workingDirTagFound = true;
                            validateWorkingDir(((String) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("User"))) {
                        if (userTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            userTagFound = true;
                            validateUser(((String) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("Application"))) {
                        if (appTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            appTagFound = true;
                            validateApplication(((ApplicationType) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("LimitOfTasks"))) {
                        if (lotTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            lotTagFound = true;
                            validateLimitOfTasks(((Integer) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("Adaptors"))) {
                        if (adaptorsTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                    "Appears more than once");
                        } else {
                            adaptorsTagFound = true;
                            validateAdaptors(((AdaptorsListType) element.getValue()));
                        }
                    } else {
                        throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute " + element.getName(),
                                "Incorrect attribute");
                    }
                }

                // Check mandatory fields have appeared
                if (!installDirTagFound) {
                    throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute InstallDir", "Doesn't appear");
                }
                if (!workingDirTagFound) {
                    throw new InvalidElementException("ComputeNode " + cn.getName(), "Attribute WorkingDir", "Doesn't appear");
                }
            } else {
                // The ComputeNode has no inner elements (and it has mandatory fields)
                throw new InvalidElementException("Project", "Attribute ComputeNode " + cn.getName(), "has no inner fields");
            }
        } else {
            // The MasterNode itself is null, raise exception
            throw new InvalidElementException("Project", "Attribute ComputeNode", "is null");
        }
    }

    /**
     * Validates a DataNode
     * 
     * @param dn
     * @throws InvalidElementException
     */
    public void validateDataNode(DataNodeType dn) throws InvalidElementException {
        if (dn != null) {
            List<AdaptorsListType> innerElements = dn.getAdaptors();
            if (innerElements != null) {
                boolean adaptorsTagFound = false;
                for (AdaptorsListType adaptors : innerElements) {
                    if (adaptorsTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("ComputeNode " + dn.getName(), "Attribute Adaptors", "Appears more than once");
                    } else {
                        adaptorsTagFound = true;
                        validateAdaptors(adaptors);
                    }
                }
            } else {
                // DataNode has no mandatory fields --> can be empty
                // Don't raise exception
            }
        } else {
            // The MasterNode itself is null, raise exception
            throw new InvalidElementException("Project", "Attribute DataNode", "is null");
        }
    }

    /**
     * Validates a Service
     * 
     * @param s
     * @throws InvalidElementException
     */
    public void validateService(ServiceType s) throws InvalidElementException {
        if (s != null) {
            // Check inner elements
            validateLimitOfTasks(s.getLimitOfTasks());
        } else {
            // The MasterNode itself is null, raise exception
            throw new InvalidElementException("Project", "Attribute Service", "is null");
        }
    }

    /**
     * Validates a full Cloud tag
     * 
     * @param c
     * @throws InvalidElementException
     */
    @SuppressWarnings("unchecked")
    public void validateCloud(CloudType c) throws InvalidElementException {
        if (c != null) {
            List<JAXBElement<?>> innerElements = c.getCloudProviderOrInitialVMsOrMinimumVMs();
            if (innerElements != null) {
                boolean cpTagFound = false;
                boolean initialVMsTagFound = false;
                boolean minVMsTagFound = false;
                boolean maxVMsTagFound = false;
                JAXBElement<Integer> initialVMs = null;
                int minVMs = -1;
                int maxVMs = -1;
                for (JAXBElement<?> element : innerElements) {
                    if (element.getName().equals(new QName("CloudProvider"))) {
                        cpTagFound = true;
                        validateCloudProvider(((CloudProviderType) element.getValue()));
                    } else if (element.getName().equals(new QName("InitialVMs"))) {
                        if (initialVMsTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("Cloud", "Attribute " + element.getName(), "Appears more than once");
                        } else {
                            initialVMsTagFound = true;
                            initialVMs = (JAXBElement<Integer>) element;
                            validateInitialVMs(((Integer) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("MinimumVMs"))) {
                        if (minVMsTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("Cloud", "Attribute " + element.getName(), "Appears more than once");
                        } else {
                            minVMsTagFound = true;
                            minVMs = ((Integer) element.getValue());
                            validateMinimumVMs(((Integer) element.getValue()));
                        }
                    } else if (element.getName().equals(new QName("MaximumVMs"))) {
                        if (maxVMsTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("Cloud", "Attribute " + element.getName(), "Appears more than once");
                        } else {
                            maxVMsTagFound = true;
                            maxVMs = ((Integer) element.getValue());
                            validateMaximumVMs(((Integer) element.getValue()));
                        }
                    } else {
                        throw new InvalidElementException("Cloud", "Attribute " + element.getName(), "Incorrect attribute");
                    }
                }

                // Check mandatory fields have appeared
                if (!cpTagFound) {
                    throw new InvalidElementException("Cloud", "Attribute CloudProvider", "Doesn't appear");
                }

                // Check coherence of initial, minimum and maximum VMs
                checkInitialMinMaxVMsCoherence(initialVMs, minVMs, maxVMs);
            } else {
                // The Cloud has no inner elements (and it has mandatory fields)
                throw new InvalidElementException("Project", "Attribute Cloud", "has no inner fields");
            }
        } else {
            // The MasterNode itself is null, raise exception
            throw new InvalidElementException("Project", "Attribute ComputeNode", "is null");
        }
    }

    /**
     * Validates a CloudProvider
     * 
     * @param cp
     * @throws InvalidElementException
     */
    public void validateCloudProvider(CloudProviderType cp) throws InvalidElementException {
        if (cp != null) {
            // Check innerElements
            List<Object> innerElements = cp.getImagesOrInstanceTypesOrLimitOfVMs();
            if (innerElements != null) {
                boolean imagesTagFound = false;
                boolean instancesTagFound = false;
                boolean lovTagFound = false;
                boolean propertiesTagFound = false;
                for (Object obj : innerElements) {
                    if (obj instanceof ImagesType) {
                        if (imagesTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("CloudProvider", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            imagesTagFound = true;
                            validateImages((ImagesType) obj);
                        }
                    } else if (obj instanceof InstanceTypesType) {
                        if (instancesTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("CloudProvider", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            instancesTagFound = true;
                            validateInstanceTypes((InstanceTypesType) obj);
                        }
                    } else if (obj instanceof Integer) { // LimitOfVMs
                        if (lovTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("CloudProvider", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            lovTagFound = true;
                            validateLimitOfVMs((Integer) obj);
                        }
                    } else if (obj instanceof CloudPropertiesType) {
                        if (propertiesTagFound) {
                            // Second occurency, throw exception
                            throw new InvalidElementException("CloudProvider", "Attribute " + obj.getClass(), "Appears more than once");
                        } else {
                            propertiesTagFound = true;
                            validateCloudProperties((CloudPropertiesType) obj);
                        }
                    } else {
                        throw new InvalidElementException("CloudProvider", "Attribute " + obj.getClass(), "Incorrect attribute");
                    }
                }

                // Check mandatory fields
                if (!imagesTagFound) {
                    throw new InvalidElementException("CloudProvider", "Attribute Images", "Doesn't appear");
                }
                if (!instancesTagFound) {
                    throw new InvalidElementException("CloudProvider", "Attribute Instances", "Doesn't appear");
                }
            } else {
                // The CloudProvider has no inner elements (and it has mandatory fields)
                throw new InvalidElementException("Cloud", "Attribute CloudProvider", "has no inner fields");
            }
        } else {
            throw new InvalidElementException("Cloud", "Attribute CloudProvider", "is null");
        }
    }

    /*
     * ********************************************** 
     * HELPERS FOR VALIDATION (PRIVATE METHODS)
     **********************************************/
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
        if (disk.getMountPoint() == null || disk.getMountPoint().isEmpty()) {
            throw new InvalidElementException("AttachedDisk" + disk.getName(), "Attribute MountPoint", "Doesn't appear");
        }
    }

    private void validatePrice(PriceType price) throws InvalidElementException {
        if (price.getTimeUnit() <= 0) {
            throw new InvalidElementException("Price", "Attribute TimeUnit", "Invalid value");
        }
        if (price.getPricePerUnit() < 0) {
            throw new InvalidElementException("Price", "Attribute PricePerUnit", "Invalid value");
        }
    }

    private void validateInstallDir(String installDir) throws InvalidElementException {
        if (installDir == null) {
            throw new InvalidElementException("ComputeNode", "Attribute installDir", "Is null");
        }
        if (installDir.isEmpty()) {
            throw new InvalidElementException("ComputeNode", "Attribute installDir", "Is empty");
        }
    }

    private void validateWorkingDir(String workingDir) throws InvalidElementException {
        if (workingDir == null) {
            throw new InvalidElementException("ComputeNode", "Attribute workingDir", "Is null");
        }
        if (workingDir.isEmpty()) {
            throw new InvalidElementException("ComputeNode", "Attribute workingDir", "Is empty");
        }
    }

    private void validateApplication(ApplicationType app) throws InvalidElementException {
        // An application can have appDir, libPath, classpath or pythonpath
        // All of the parameters are optional and XSD ensures that they only appear once
        // Nothing to do
    }

    private void validateLimitOfTasks(Integer lot) throws InvalidElementException {
        if (lot != null) {
            if (((int) lot) < 0) {
                throw new InvalidElementException("ComputeNode", "Attribute LimitOfTasks", "has an invalid value");
            }

            if (((int) lot) == 0) {
                logger.warn("ComputeNode has LimitOfTasks = 0, no task will be scheduled to this resource");
            }
        } else {
            // LimitOfTasks is always optional
            // Nothing to do
        }
    }

    private void validateInitialVMs(Integer initialVMs) throws InvalidElementException {
        if (initialVMs == null) {
            throw new InvalidElementException("Cloud", "Attribute initialVMs", "Is null");
        }
        if (((int) initialVMs) < 0) {
            throw new InvalidElementException("ComputeNode", "Attribute initialVMs", "has an invalid value");
        }
    }

    private void validateMinimumVMs(Integer minimumVMs) throws InvalidElementException {
        if (minimumVMs == null) {
            throw new InvalidElementException("Cloud", "Attribute minimumVMs", "Is null");
        }
        if (((int) minimumVMs) < 0) {
            throw new InvalidElementException("ComputeNode", "Attribute minimumVMs", "has an invalid value");
        }
    }

    private void validateMaximumVMs(Integer maximumVMs) throws InvalidElementException {
        if (maximumVMs == null) {
            throw new InvalidElementException("Cloud", "Attribute maximumVMs", "Is null");
        }
        if (((int) maximumVMs) < 0) {
            throw new InvalidElementException("ComputeNode", "Attribute maximumVMs", "has an invalid value");
        }
    }

    private void checkInitialMinMaxVMsCoherence(JAXBElement<Integer> initialVMs, int minVMs, int maxVMs) throws InvalidElementException {
        // Get real values
        int initial = -1;
        if (initialVMs != null) {
            initial = initialVMs.getValue();
        }

        // Check
        if (minVMs >= 0 && maxVMs >= 0 && minVMs > maxVMs) {
            throw new InvalidElementException("Project", "Attributes minVMs/maxVMs", "define empty range");
        }

        if (minVMs >= 0 && initial >= 0 && initial < minVMs) {
            // Inconsistent but error can be recovered. Raise warning
            initialVMs.setValue(((Integer) minVMs));
            logger.warn("InitialVMs value was " + initial + " but it is set to " + minVMs + " because it is the minimumVMs value");
        }

        if (maxVMs >= 0 && initial >= 0 && initial > maxVMs) {
            // Inconsistent but error can be recovered. Raise warning
            initialVMs.setValue(((Integer) maxVMs));
            logger.warn("InitialVMs value was " + initial + " but it is set to " + maxVMs + " because it is the maximumVMs value");
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
        List<JAXBElement<?>> innerElements = image.getInstallDirOrWorkingDirOrUser();
        if (innerElements != null) {
            boolean installDirTagFound = false;
            boolean workingDirTagFound = false;
            boolean userTagFound = false;
            boolean appTagFound = false;
            boolean lotTagFound = false;
            boolean adaptorsTagFound = false;
            for (JAXBElement<?> element : innerElements) {
                if (element.getName().equals(new QName("InstallDir"))) {
                    if (installDirTagFound) {
                        // Second appearance
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        installDirTagFound = true;
                        validateInstallDir((String) element.getValue());
                    }
                } else if (element.getName().equals(new QName("WorkingDir"))) {
                    if (workingDirTagFound) {
                        // Second appearance
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        workingDirTagFound = true;
                        validateWorkingDir((String) element.getValue());
                    }
                } else if (element.getName().equals(new QName("User"))) {
                    if (userTagFound) {
                        // Second occurency, throw exception
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        userTagFound = true;
                        validateUser(((String) element.getValue()));
                    }
                } else if (element.getName().equals(new QName("Application"))) {
                    if (appTagFound) {
                        // Second appearance
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        appTagFound = true;
                        validateApplication((ApplicationType) element.getValue());
                    }
                } else if (element.getName().equals(new QName("LimitOfTasks"))) {
                    if (lotTagFound) {
                        // Second appearance
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        lotTagFound = true;
                        validateLimitOfTasks((Integer) element.getValue());
                    }
                } else if (element.getName().equals(new QName("Package"))) {
                    validatePackage((PackageType) element.getValue());
                } else if (element.getName().equals(new QName("Adaptors"))) {
                    if (adaptorsTagFound) {
                        // Second appearance
                        throw new InvalidElementException("Image", "Attribute " + element.getName(), "Appears more than once");
                    } else {
                        adaptorsTagFound = true;
                        validateAdaptors((AdaptorsListType) element.getValue());
                    }
                } else {
                    throw new InvalidElementException("Image", "Attribute " + element.getName(), "Incorrect attribute");
                }
            }
            // Check minimum appearences
            if (!installDirTagFound) {
                throw new InvalidElementException("Image ", "Attribute installDir", "Doesn't appear");
            }
            if (!workingDirTagFound) {
                throw new InvalidElementException("Image ", "Attribute workingDir", "Doesn't appear");
            }
        } else {
            // Empty inner elements
            throw new InvalidElementException("Image", "", "Content is empty");
        }
    }

    private void validateUser(String username) throws InvalidElementException {
        // The username is a user selected string that cannot be validated
        // Nothing to do
    }

    private void validatePackage(PackageType pack) throws InvalidElementException {
        if (pack != null) {
            // Check mandatory elements
            if (pack.getSource() == null) {
                throw new InvalidElementException("Package", "Attribute source", "Is null");
            }
            if (pack.getSource().isEmpty()) {
                throw new InvalidElementException("Package", "Attribute source", "Is empty");
            }

            if (pack.getTarget() == null) {
                throw new InvalidElementException("Package", "Attribute source", "Is null");
            }
            if (pack.getTarget().isEmpty()) {
                throw new InvalidElementException("Package", "Attribute source", "Is empty");
            }

            // Check optional elements
            if (pack.getIncludedSoftware() != null) {
                validateSoftwareList(pack.getIncludedSoftware());
            }
        } else {
            throw new InvalidElementException("Image", "Attribute Package", "is null");
        }
    }

    private void validateSoftwareList(SoftwareListType softwares) throws InvalidElementException {
        if (softwares != null) {
            // Check inner elements
            List<String> innerElements = softwares.getApplication();
            for (String app : innerElements) {
                validateApplication(app);
            }
        } else {
            throw new InvalidElementException("Package", "Attribute SoftwareList", "is null");
        }
    }

    private void validateApplication(String app) throws InvalidElementException {
        // Application only contains a name (String) defined by user
        // Nothing to check
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
        // An instance can only have attribute name (that is XSD validated)
        // Nothing to do
    }

    private void validateCloudProperties(CloudPropertiesType properties) throws InvalidElementException {
        if (properties != null) {
            // Check inner elements
            List<CloudPropertyType> innerElements = properties.getProperty();
            if (innerElements != null) {
                for (CloudPropertyType prop : innerElements) {
                    validateCloudProperty(prop);
                }
            } else {
                // The <Property> inner element is not mandatory.
                // Nothing to do
            }
        } else {
            throw new InvalidElementException("Cloud", "Attribute Properties", "is null");
        }
    }

    private void validateCloudProperty(CloudPropertyType prop) throws InvalidElementException {
        if (prop != null) {
            // Check mandatory elements
            if (prop.getName() == null) {
                throw new InvalidElementException("Package", "Attribute source", "Is null");
            }
            if (prop.getName().isEmpty()) {
                throw new InvalidElementException("Package", "Attribute source", "Is empty");
            }

            if (prop.getValue() == null) {
                throw new InvalidElementException("Package", "Attribute source", "Is null");
            }
            if (prop.getValue().isEmpty()) {
                // The value tag must exist but can be empty, nothing to do
            }

            // Check optional elements
            // None
        } else {
            throw new InvalidElementException("Cloud Properties", "Attribute Property", "is null");
        }
    }

    private void validateLimitOfVMs(Integer lovms) throws InvalidElementException {
        if (lovms == null) {
            throw new InvalidElementException("CloudProvider", "Attribute LimitOfVMs", "Is null");
        }
        if (((int) lovms) < 0) {
            throw new InvalidElementException("CloudProvider", "Attribute LimitOfVMs", "has an invalid value");
        }

        if (((int) lovms) == 0) {
            logger.warn("CloudProvider has LimitOfVMs = 0, no task will be scheduled to this resource");
        }
    }

}