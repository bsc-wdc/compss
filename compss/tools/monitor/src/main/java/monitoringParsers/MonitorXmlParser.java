package monitoringParsers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bsc.compss.commons.Loggers;
import com.bsc.compss.ui.Constants;
import com.bsc.compss.ui.Properties;


public class MonitorXmlParser {

    private static List<String[]> workersDataArray;
    private static List<String[]> coresDataArray;
    private static HashMap<String, String> statisticParameters;

    private static final Logger logger = LogManager.getLogger(Loggers.COMPSS_STATE_XML_PARSER);


    public static List<String[]> getWorkersDataArray() {
        logger.debug("Granting access to resources data");
        return workersDataArray;
    }

    public static List<String[]> getCoresDataArray() {
        logger.debug("Granting access to cores data");
        return coresDataArray;
    }

    public static HashMap<String, String> getStatisticsParameters() {
        return statisticParameters;
    }

    public static void parseResources() {
        String monitorLocation = Properties.getBasePath() + Constants.MONITOR_XML_FILE;
        logger.debug("Parsing XML file...");
        // Reset attribute
        workersDataArray = new ArrayList<>();

        // Show monitor location
        logger.debug("Monitor Location : " + monitorLocation);

        // Compute attribute
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
            NodeList nl = resourcesDoc.getChildNodes();
            Node COMPSs = null;
            for (int i = 0; i < nl.getLength(); i++) {
                if (nl.item(i).getNodeName().equals("COMPSsState")) {
                    COMPSs = nl.item(i);
                    break;
                }
            }

            if (COMPSs == null) {
                // NO COMPSs item --> empty
                return;
            }

            nl = COMPSs.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);
                if (n.getNodeName().equals("ResourceInfo")) {
                    workersDataArray = parseResourceInfoNode(n);
                }
            }
        } catch (Exception e) {
            logger.error("Cannot load monitor xml files", e);
            // e.printStackTrace();
            return;
        }

        logger.debug("Success: Parse finished");
    }

    public static void parseCores() {
        String monitorLocation = Properties.getBasePath() + Constants.MONITOR_XML_FILE;
        logger.debug("Parsing XML file...");
        // Reset attribute
        coresDataArray = new ArrayList<>();

        // Show monitor location
        logger.debug("Monitor Location : " + monitorLocation);

        // Compute attributes
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
            NodeList nl = resourcesDoc.getChildNodes();
            Node COMPSs = null;
            for (int i = 0; i < nl.getLength(); i++) {
                if (nl.item(i).getNodeName().equals("COMPSsState")) {
                    COMPSs = nl.item(i);
                    break;
                }
            }

            if (COMPSs == null) {
                // NO COMPSs item --> empty
                return;
            }

            nl = COMPSs.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);
                if (n.getNodeName().equals("CoresInfo")) {
                    coresDataArray = parseCoresInfoNode(n);
                }
            }
        } catch (Exception e) {
            logger.error("Cannot load monitor xml files", e);
            return;
        }

        logger.debug("Success: Parse finished");
    }

    public static void parseStatistics() {
        String monitorLocation = Properties.getBasePath() + Constants.MONITOR_XML_FILE;
        logger.debug("Parsing XML file for statistics...");
        // Reset attribute
        statisticParameters = new HashMap<>();

        // Show monitor location
        logger.debug("Monitor Location : " + monitorLocation);

        // Compute attribute
        try {
            DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
            docFactory.setNamespaceAware(true);
            Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
            NodeList nl = resourcesDoc.getChildNodes();
            Node COMPSs = null;
            for (int i = 0; i < nl.getLength(); i++) {
                if (nl.item(i).getNodeName().equals("COMPSsState")) {
                    COMPSs = nl.item(i);
                    break;
                }
            }

            if (COMPSs == null) {
                // NO COMPSs item --> empty
                return;
            }

            nl = COMPSs.getChildNodes();
            for (int i = 0; i < nl.getLength(); i++) {
                Node n = nl.item(i);
                if (n.getNodeName().equals("Statistics")) {
                    statisticParameters = parseStatisticsNode(n);
                }
            }
        } catch (Exception e) {
            logger.error("Cannot load monitor xml files", e);
            return;
        }

        logger.debug("Success: Parse finished");
    }

    private static List<String[]> parseResourceInfoNode(Node resourceInfo) throws Exception {
        logger.debug("Parsing resources nodes...");
        List<String[]> datas = new ArrayList<>();
        NodeList nl = resourceInfo.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeName().equals("Resource")) {
                datas.add(parseResourceNode(n));
            }
        }
        return datas;
    }

    private static String[] parseResourceNode(Node resource) throws Exception {
        String workerName = resource.getAttributes().getNamedItem("id").getTextContent();
        logger.debug("Parse ResourceNode " + workerName);

        final int MAX_PARAMS = 11;
        String[] data = new String[MAX_PARAMS];
        // workerName, totalCPUu, totalGPUu, totalFPGAu, totalOTHERu,
        // memory, disk, status, provider, image, actions
        for (int i = 0; i < data.length; ++i) {
            data[i] = "-";
        }
        data[0] = workerName;

        NodeList nl = resource.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node field = nl.item(i);
            logger.debug("Parsing item: " + field.getNodeName());
            switch (field.getNodeName()) {
                case "TotalCPUComputingUnits":
                    String content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Integer.valueOf(content) > 0) {
                        data[1] = content;
                    }
                    break;
                case "TotalGPUComputingUnits":
                    content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Integer.valueOf(content) > 0) {
                        data[2] = content;
                    }
                    break;
                case "TotalFPGAComputingUnits":
                    content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Integer.valueOf(content) > 0) {
                        data[3] = content;
                    }
                    break;
                case "TotalOTHERComputingUnits":
                    content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Integer.valueOf(content) > 0) {
                        data[4] = content;
                    }
                    break;
                case "Memory":
                    content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Float.valueOf(content) > 0f) {
                        data[5] = content;
                    }
                    break;
                case "Disk":
                    content = field.getTextContent();
                    if (content != null && !content.isEmpty() && Float.valueOf(content) > 0) {
                        data[6] = content;
                    }
                    break;
                case "Status":
                    data[7] = field.getTextContent();
                    break;
                case "Provider":
                    data[8] = field.getTextContent();
                    break;
                case "Image":
                    data[9] = field.getTextContent();
                    break;
                case "Actions":
                    data[10] = parseActions(field);
                    break;
                case "#text":
                    //Nothing to do
                    break;
                default:
                    logger.error("Unrecognised field on ResourceNode " + field.getNodeName());
            }
        }
        return data;
    }

    private static String parseActions(Node actions) {
        StringBuilder taskIds = new StringBuilder();
        NodeList nl = actions.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeName().equals("Action")) {
                /*
                 * An action is of the form:
                 *      - "ExecutionAction ( Task tid, CE name ceName)"
                 *      - "StartWorkerAction ( Worker workerName)"
                 */
                String[] actionInfo = n.getTextContent().split(" ");
                String actionType = actionInfo[2];
                switch(actionType) {
                    case "Task":
                        String taskId = actionInfo[3];
                        // Remove , char
                        taskId = taskId.substring(0, taskId.length() - 1);
                        taskIds.append(taskId).append(" ");
                        break;
                    case "Worker":
                        String workerName = actionInfo[3];
                        // Remove ) char
                        workerName = workerName.substring(0, workerName.length() - 1);
                        taskIds.append("Starting worker ").append(workerName).append(" ");
                        break;
                    default:
                        // Nothing to do
                        break;
                }
            }
        }
        return taskIds.toString();
    }

    private static List<String[]> parseCoresInfoNode(Node coresInfo) throws Exception {
        logger.debug("Parsing cores nodes...");
        List<String[]> datas = new ArrayList<>();
        NodeList nl = coresInfo.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node n = nl.item(i);
            if (n.getNodeName().equals("Core")) {
                datas.addAll(parseCoreNode(n));
            }
        }
        return datas;
    }

    private static List<String[]> parseCoreNode(Node cores) throws Exception {
        String coreId = cores.getAttributes().getNamedItem("id").getTextContent();
        logger.debug("  - Parsing coreId " + coreId);

        NodeList impls = cores.getChildNodes();
        List<String[]> data = new ArrayList<>();
        for (int i = 0; i < impls.getLength(); ++i) {
            Node impl = impls.item(i);
            if (impl.getNodeName().equals("Impl")) {
                data.add(parseImplNode(impl, coreId));
            }
        }

        return data;
    }

    private static String[] parseImplNode(Node impl, String coreId) {
        String implId = impl.getAttributes().getNamedItem("id").getTextContent();
        logger.debug("     - Parsing implId " + implId);

        final int MAX_PARAMS = 7;
        String[] data = new String[MAX_PARAMS];
        // coreId, implId, signature, meanET, minET, maxET, execCount
        for (int i = 0; i < data.length; ++i) {
            data[i] = "-";
        }
        data[0] = coreId;
        data[1] = implId;

        NodeList nl = impl.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node field = nl.item(i);
            switch (field.getNodeName()) {
                case "Signature":
                    data[2] = field.getTextContent();
                    break;
                case "MeanExecutionTime":
                    Long ms = Long.valueOf(field.getTextContent());
                    data[3] = String.valueOf((float) (ms) / (float) (1_000));
                    break;
                case "MinExecutionTime":
                    ms = Long.valueOf(field.getTextContent());
                    data[4] = String.valueOf((float) (ms) / (float) (1_000));
                    break;
                case "MaxExecutionTime":
                    ms = Long.valueOf(field.getTextContent());
                    data[5] = String.valueOf((float) (ms) / (float) (1_000));
                    break;
                case "ExecutedCount":
                    int execCount = Integer.valueOf(field.getTextContent());
                    if (execCount == 0) {
                        // If no exec, reset min,max,mean timers
                        data[3] = "-";
                        data[4] = "-";
                        data[5] = "-";
                    }
                    data[6] = String.valueOf(execCount);
                    break;
                case "#text":
                    //Nothing to do
                    break;
                default:
                    logger.error("Unrecognised field on ImplNode " + field.getNodeName());
            }
        }

        return data;
    }
    
    private static HashMap<String, String> parseStatisticsNode(Node statistics) throws Exception {
        logger.debug("  - Parsing statistics");

        NodeList statisticValues = statistics.getChildNodes();
        HashMap<String, String> data = new HashMap<>();
        for (int i = 0; i < statisticValues.getLength(); ++i) {
            Node statisticNode = statisticValues.item(i);
            if (statisticNode.getNodeName().equals("Statistic")) {
                String[] statisticKeyValue = parseStatisticNode(statisticNode);
                data.put(statisticKeyValue[0], statisticKeyValue[1]);
            }
        }

        return data;
    }
    
    private static String[] parseStatisticNode(Node statistic) {
        final int MAX_PARAMS = 2;
        String[] entry = new String[MAX_PARAMS];    // key,value
        NodeList nl = statistic.getChildNodes();
        for (int i = 0; i < nl.getLength(); i++) {
            Node field = nl.item(i);
            switch (field.getNodeName()) {
                case "Key":
                    entry[0] = field.getTextContent();
                    break;
                case "Value":
                    entry[1] = field.getTextContent();
                    break;
                case "#text":
                    //Nothing to do
                    break;
                default:
                    logger.error("Unrecognised field on StatisticNode " + field.getNodeName());
            }
        }
        
        return entry;
    }

}
