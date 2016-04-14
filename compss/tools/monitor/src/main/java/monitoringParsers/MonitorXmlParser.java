package monitoringParsers;

import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.apache.log4j.Logger;

import com.bsc.compss.ui.Constants;
import com.bsc.compss.ui.Properties;


public class MonitorXmlParser {
	private static List<String[]> WorkersDataArray;
	private static List<String[]> CoresDataArray;
	private static String[] statisticsParameters;
	private static final Logger logger = Logger.getLogger("compssMonitor.monitoringParser");
	
	public static List<String[]> getWorkersDataArray() {
		logger.debug("Granting access to resources data");
		return WorkersDataArray;
	}
	
	public static List<String[]> getCoresDataArray() {
		logger.debug("Granting access to cores data");
		return CoresDataArray;
	}
	
	public static String[] getStatisticsParameters() {
		return statisticsParameters;
	}
	
	public static void parseResources () {
		String monitorLocation = Properties.BASE_PATH + Constants.MONITOR_XML_FILE;
		logger.debug("Parsing XML file...");
		//Reset attribute
		WorkersDataArray = new ArrayList<String[]>();

		//Show monitor location
		logger.debug("Monitor Location : " + monitorLocation);
		
		//Compute attribute
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
			NodeList nl = resourcesDoc.getChildNodes();
			Node COMPSs = null;
			for (int i = 0; i < nl.getLength(); i++) {
				if (nl.item(i).getNodeName().equals("COMPSsState")){
					COMPSs = nl.item(i);
					break;
				}	
			}
			
			if (COMPSs == null){
				//NO COMPSs item --> empty
				return;
			}
			
			nl = COMPSs.getChildNodes();
			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);
				if (n.getNodeName().equals("ResourceInfo")) {
					WorkersDataArray = parseResourceInfoNode(n);
				}
			}
		} catch (Exception e) {
			logger.error("Cannot load monitor xml files");
			//e.printStackTrace();
			return;
		}
		
		logger.debug("Success: Parse finished");
	}
	
	public static void parseCores () {
		String monitorLocation = Properties.BASE_PATH + Constants.MONITOR_XML_FILE;
		logger.debug("Parsing XML file...");
		//Reset attribute
		CoresDataArray = new ArrayList<String[]>();

		//Show monitor location
		logger.debug("Monitor Location : " + monitorLocation);
		
		//Compute attributes
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
			NodeList nl = resourcesDoc.getChildNodes();
			Node COMPSs = null;
			for (int i = 0; i < nl.getLength(); i++) {
				if (nl.item(i).getNodeName().equals("COMPSsState")){
					COMPSs = nl.item(i);
					break;
				}	
			}
			
			if (COMPSs == null){
				//NO COMPSs item --> empty
				return;
			}
			
			nl = COMPSs.getChildNodes();
			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);
				if (n.getNodeName().equals("CoresInfo")) {
					CoresDataArray = parseCoresInfoNode(n);
				}
			}
		} catch (Exception e) {
			logger.error("Cannot load monitor xml files");
			return;
		}
		
		logger.debug("Success: Parse finished");
	}
	
	public static void parseStatistics () {
		String monitorLocation = Properties.BASE_PATH + Constants.MONITOR_XML_FILE;
		logger.debug("Parsing XML file for statistics...");
		//Reset attribute
		statisticsParameters = new String[1];

		//Show monitor location
		logger.debug("Monitor Location : " + monitorLocation);
		
		//Compute attribute
		try {
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			docFactory.setNamespaceAware(true);
			Document resourcesDoc = docFactory.newDocumentBuilder().parse(monitorLocation);
			NodeList nl = resourcesDoc.getChildNodes();
			Node COMPSs = null;
			for (int i = 0; i < nl.getLength(); i++) {
				if (nl.item(i).getNodeName().equals("COMPSsState")){
					COMPSs = nl.item(i);
					break;
				}	
			}
			
			if (COMPSs == null){
				//NO COMPSs item --> empty
				return;
			}
			
			nl = COMPSs.getChildNodes();
			for (int i = 0; i < nl.getLength(); i++) {
				Node n = nl.item(i);
				if (n.getNodeName().equals("AccumulatedCost")) {
					statisticsParameters[0] = n.getTextContent();
				}
			}
		} catch (Exception e) {
			logger.error("Cannot load monitor xml files");
			return;
		}
		
		logger.debug("Success: Parse finished");
	}
	

	private static List<String[]> parseResourceInfoNode(Node resourceInfo) throws Exception {
		logger.debug("Parsing resources nodes...");
		List<String[]> datas = new ArrayList<String[]>();
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
		String[] data = new String[9];
		data[0] = resource.getAttributes().getNamedItem("id").getTextContent();
		NodeList nl = resource.getChildNodes();
		for (int i = 0; i < nl.getLength(); i++) {
			Node n = nl.item(i);
			if (n.getNodeName().equals("CPU")) {
				data[1] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Core")) {
				data[2] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Memory")) {
				data[3] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Disk")) {
				data[4] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Provider")) {
				data[5] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Image")) {
				data[6] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Status")) {
				data[7] = n.getTextContent();
			}
			else if (n.getNodeName().equals("Tasks")) {
				data[8] = n.getTextContent();
			}
		}
		return data;
	}

	private static List<String[]> parseCoresInfoNode(Node coresInfo) throws Exception {
		logger.debug("Parsing cores nodes...");
		List<String[]> datas = new ArrayList<String[]>();
		NodeList nl = coresInfo.getChildNodes();
		for (int i = 0; i < nl.getLength(); i++) {
			Node n = nl.item(i);
			if (n.getNodeName().equals("Core")) {
				datas.add(parseCoreNode(n));
			}
		}
		return datas;
	}
	
	private static String[] parseCoreNode(Node cores) throws Exception {
		String[] data = new String[5];
		data[0] = cores.getAttributes().getNamedItem("id").getTextContent();
		String signature = cores.getAttributes().getNamedItem("signature").getTextContent();
		int pos = signature.indexOf("(");
		int posfin = signature.indexOf(")");
		data[1] = signature.substring(0, pos);
		data[2] = signature.substring(pos + 1, posfin);
		NodeList nl = cores.getChildNodes();
		for (int i = 0; i < nl.getLength(); i++) {
			Node n = nl.item(i);
			if (n.getNodeName().equals("MeanExecutionTime")) {
				int ms = Integer.valueOf(n.getTextContent());
				data[3] = String.valueOf((float)(ms) / (float)(1000));
			} else if (n.getNodeName().equals("ExecutedCount")) {
				data[4] = n.getTextContent();
			}
		}
		return data;
	}

}
