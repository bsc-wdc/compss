package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.types.AdaptorDescription;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.xpath.domapi.XPathEvaluatorImpl;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.xpath.XPathEvaluator;
import org.w3c.dom.xpath.XPathResult;
import org.xml.sax.SAXException;


/**
 * The ProjectManager class is an utility to manage the configuration of all the
 * workers
 */
public class ProjectManager {

    private static Document projectDoc;
    private static XPathEvaluator evaluator;
   
    

    /**
     * Initializes the ProjectManager with the values on the project file
     *
     * @throws Exception Error on the project file parsing
     */
    public static void init() throws Exception {
        String projectFile = System.getProperty(ITConstants.IT_PROJ_FILE);

        // Parse the XML document which contains resource information
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);
        projectDoc = docFactory.newDocumentBuilder().parse(projectFile);

        // Validate the document against an XML Schema
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Source schemaFile = new StreamSource(System.getProperty(ITConstants.IT_PROJ_SCHEMA));
        Schema schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        // validate the DOM tree
        try {
            validator.validate(new DOMSource(projectDoc));
        } catch (SAXException e) {
            System.err.println("Error Validating project.xml config file.\n" + e.getMessage());   
        } catch (IOException e){
            System.err.println("IOError while trying to validate project.xml config file.\n" + e.getMessage());
        }
        // Create an XPath evaluator to solve queries
        evaluator = new XPathEvaluatorImpl(projectDoc);

        //LOADING WORKERS
        //workers = new HashMap<String, ProjectWorker>();

        // Find all the workers defined in the project file
        String xPathToWorkers = "/Project/Worker";
        XPathResult workerRes = (XPathResult) evaluator.evaluate(xPathToWorkers, projectDoc, /*resolver*/ null,
                XPathResult.UNORDERED_NODE_ITERATOR_TYPE, null);
    }
    
    public static TreeMap<String, AdaptorDescription> parseAdaptors(){
        TreeMap<String, AdaptorDescription> adaptorsDesc = new TreeMap<String, AdaptorDescription>();

        int[] nioPorts = new int[2];
        int[] gatPorts = new int[2];
        
        nioPorts = getAdaptorMinMaxPort(AdaptorDescription.NIOAdaptor, evaluator, projectDoc);
        gatPorts = getAdaptorMinMaxPort(AdaptorDescription.GATAdaptor, evaluator, projectDoc);            

        adaptorsDesc.put(AdaptorDescription.NIOAdaptor, new AdaptorDescription(AdaptorDescription.NIOAdaptor, nioPorts[0], nioPorts[1], null));
        adaptorsDesc.put(AdaptorDescription.GATAdaptor, new AdaptorDescription(AdaptorDescription.GATAdaptor, gatPorts[0], gatPorts[1], null));

        return adaptorsDesc;
    }
    
    public static int[] getAdaptorMinMaxPort(String adaptorName, XPathEvaluator evaluator, Document projectDoc){
        int[] ports = new int[2];
        ports[0] = -1;
        ports[1] = -1;
        
        String basePath = "/Project/Worker/Adaptors";
        String path = basePath + "/Adaptor[@name='" + adaptorName + "']";
        
        XPathResult res;
        Node n;
        res = (XPathResult) evaluator.evaluate(path + "/MinPort", projectDoc, null,
                XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        n = res.getSingleNodeValue();

        try {
            ports[0] = Integer.parseInt(n.getTextContent());
        } catch (Exception e){
            //No min port found (or correctly parsed) on project.xml, as it's not mandatory don't raise exception 
            //System.out.println("No min port defined for " + adaptorName + " on project.xml.");
        }    
        
       res = (XPathResult) evaluator.evaluate(path + "/MaxPort", projectDoc, null,
                XPathResult.FIRST_ORDERED_NODE_TYPE, null);
       
        n = res.getSingleNodeValue();
 
        try {
            ports[1] = Integer.parseInt(n.getTextContent());
        } catch (Exception e){
            //No min port found (or correctly parsed) on project.xml, as it's not mandatory don't raise exception 
            //System.out.println("No max port defined for " + adaptorName + " on project.xml.");
        }    


        return ports;
    }

    /**
     * Checks if there is a resource with identifier name inside the project
     * file
     *
     * @param name identifier
     * @return true if there is a configuration on the project file for a
     * resource with identifier name
     */
    public static boolean containsWorker(String name) {
        String xPathToService = "/Project/Worker[@Name='" + name + "']";
        XPathResult res = (XPathResult) evaluator.evaluate(xPathToService, projectDoc, null,
                XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        Node n = res.getSingleNodeValue();
        
        return (n != null);
    }

    public static HashMap<String, String> getWorkerProperties(String name) {
        HashMap<String, String> properties = new HashMap<String, String>();

        String xPathToService = "/Project/Worker[@Name='" + name + "']";
        XPathResult worker = (XPathResult) evaluator.evaluate(xPathToService,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        
        Node workerNode = worker.getSingleNodeValue();
        for (int j = 0; j < workerNode.getChildNodes().getLength(); j++) {
            Node n = workerNode.getChildNodes().item(j);
            String propName = n.getNodeName();
            if (propName.compareTo("#text") != 0 && propName.compareTo("#comment") != 0) {
                String propValue = n.getTextContent();
                properties.put(propName, propValue);
            }
        }
        
        return properties;
    }

    /**
     * Returns the value of a resource property set in the project file
     *
     * @param workerName name of the resource
     * @param property name of the property
     * @return value of that resource property on the XML project file
     */
    private static String getResourcePropertyInit(String workerName, String property) {
        String xPathToProp = "/Project/Worker[@Name='" + workerName + "']/" + property;
        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        
        Node n = res.getSingleNodeValue();
        if (n == null) {
            return null;
        } else {
            return n.getTextContent();
        }
    }

    /**
     * Checks if a any cloud Provider with that name appears on the project file
     *
     * @param cloudName Name of the cloud provider
     * @return true if the provider exists
     */
    public static boolean existsCloudProvider(String cloudName) {
        String xPathToProp = "/Project/Cloud/Provider[@name='" + cloudName + "']";

        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        
        Node n = res.getSingleNodeValue();
        return (n != null);
    }

    /**
     * Returns the value of a property for the whole cloud
     *
     * @param property Name of the property
     * @return value of that cloud property
     */
    public static String getCloudProperty(String property) {
        String xPathToProp = "/Project/Cloud/" + property;
        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        
        Node n = res.getSingleNodeValue();
        if (n == null) {
            return null;
        } else {
            return n.getTextContent();
        }
    }

    /**
     * Reads from the project file the max amount of VMs that can be created on
     * a cloud provider at the same time
     *
     * @param cloudName name of the cloud
     * @return limit of VMs that can be running at the same time
     */
    public static Integer getCloudProviderLimitOfVMs(String cloudName) {

        String xPathToProp = "/Project/Cloud/Provider[@name='" + cloudName + "']/LimitOfVMs";
        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,  projectDoc, null,
                XPathResult.UNORDERED_NODE_ITERATOR_TYPE, null);
        
        Node n = res.iterateNext();
        if (n != null) {
            return Integer.parseInt(n.getTextContent());
        }

        return null;
    }

    /**
     * Reads from the project file the value of all properties of a resource
     *
     * @param cloudName name of the cloud
     * @return HashMap with the pairs name-value for each property of the cloud
     * with that name
     */
    public static HashMap<String, String> getCloudProviderProperties(String cloudName) {
        String xPathToProp = "/Project/Cloud/Provider[@name='" + cloudName + "']/Property";

        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc,  null, XPathResult.UNORDERED_NODE_ITERATOR_TYPE, null);
        HashMap<String, String> properties = new HashMap<String, String>();
        
        Node n = res.iterateNext();
        while (n != null) {
            String name = "";
            String value = "";
            for (int i = 0; i < n.getChildNodes().getLength(); i++) {
                Node child = n.getChildNodes().item(i);
                if (child.getNodeName().compareTo("Name") == 0) {
                    name = child.getTextContent();
                } else if (child.getNodeName().compareTo("Value") == 0) {
                    value = child.getTextContent();
                }
            }
            properties.put(name, value);
            if (n.getAttributes().getNamedItem("context") != null) {
                String context = n.getAttributes().getNamedItem("context").getTextContent();
                properties.put("[context=" + context + "]" + name, value);
                if (context.compareTo("file") == 0) {
                    properties.put("[context=file]" + name, value);
                } else if (context.compareTo("job") == 0) {
                    properties.put("[context=job]" + name, value);
                } else {
                    properties.put("[context=file]" + name, value);
                    properties.put("[context=job]" + name, value);
                }
            }
            n = res.iterateNext();
        }
        
        return properties;
    }

    /**
     * Checks on the project file if a cloud provider can create machines using
     * a certain image
     *
     * @param cloudProvider name of the cloud provider
     * @param imageName name of the image
     * @return true if the provider can create VMs with that image
     */
    public static Node existsImageOnProvider(String cloudProvider, String imageName) {
        String xPathToProp = "/Project/Cloud/Provider[@name='" + cloudProvider + "']/ImageList/Image[@name='" + imageName + "']";

        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        Node n = res.getSingleNodeValue();
        
        return n;
    }

    /**
     * Checks on the project file if a cloud provider can create certain kind of
     * instance
     *
     * @param cloudProvider name of the cloud provider
     * @param instanceName name of the image
     * @return true if the provider can create VMs with that image
     */
    public static Node existsInstanceTypeOnProvider(String cloudProvider, String instanceName) {
        String xPathToProp = "/Project/Cloud/Provider[@name='" + cloudProvider + "']/InstanceTypes/Resource[@name='" + instanceName + "']";

        XPathResult res = (XPathResult) evaluator.evaluate(xPathToProp,
                projectDoc, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
        Node n = res.getSingleNodeValue();
        
        return n;
    }

    /**
     * Checks if the ProjectManager has already been initialized
     *
     * @return true if it is initialized
     */
    public static boolean isInit() {
        return projectDoc != null;
    }


    /**
     * Reloads the project xml file
     *
     * @throws Exception
     */
    public static void refresh() throws Exception {
        String projectFile = System.getProperty(ITConstants.IT_PROJ_FILE);

        // Parse the XML document which contains resource information
        DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
        docFactory.setNamespaceAware(true);
        Document tempDoc = docFactory.newDocumentBuilder().parse(projectFile);

        // Validate the document against an XML Schema
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        Source schemaFile = new StreamSource(System.getProperty(ITConstants.IT_PROJ_SCHEMA));
        Schema schema = schemaFactory.newSchema(schemaFile);
        Validator validator = schema.newValidator();
        validator.validate(new DOMSource(tempDoc));

        // Create an XPath evaluator to solve queries
        evaluator = new XPathEvaluatorImpl(tempDoc);
        projectDoc = tempDoc;
    }
}
