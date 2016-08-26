package integratedtoolkit.connectors.vmm;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.CDATASection;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.SAXException;


class VMTemplate {

	private static final String IMAGE_TAG = "IMAGE";
	private static final String CONTEXT_TAG = "CONTEXT";
	private static final String PUBKEY_TAG = "SSH_PUBLIC_KEY";
	private static final String TEMPLATE_TAG = "TEMPLATE";
	private static final String DISK_TAG = "DISK";

	private Document template;


	VMTemplate(String temp) throws SAXException, IOException, ParserConfigurationException {
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		DocumentBuilder db = dbf.newDocumentBuilder();
		this.template = db.parse(new ByteArrayInputStream(temp.getBytes()));
	}

	String getString() throws ClassCastException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		DOMImplementationRegistry reg = DOMImplementationRegistry.newInstance();
		DOMImplementationLS impl = (DOMImplementationLS) reg.getDOMImplementation("LS");
		LSSerializer serializer = impl.createLSSerializer();
		serializer.getDomConfig().setParameter("xml-declaration", false);

		return serializer.writeToString(this.template.getElementsByTagName(TEMPLATE_TAG).item(0));
	}

	// String getHomeDevice() {
	// if (homeDev != null)
	// return homeDev;
	//
	// Element e = template.getDocumentElement();
	// NodeList nl = e.getElementsByTagName(HOME_TAG);
	//
	// homeDev = nl.item(0).getTextContent();
	//
	// return homeDev;
	// }

	// void setMemory(int memory) {
	// setElement(MEM_TAG, String.valueOf(memory));
	// }
	//
	// void setCPU(int cpu) {
	// setElement(VCPU_TAG, String.valueOf(cpu));
	// setElement(CPU_TAG, String.valueOf(cpu));
	// }
	//
	// void setUser(String user) {
	// setContextElement(USER_TAG, user);
	// }

	void setImage(String name) {
		Element e = template.getDocumentElement();
		Node tempNode = e.getElementsByTagName(TEMPLATE_TAG).item(0);
		Node firstDisk = e.getElementsByTagName(DISK_TAG).item(0);

		Element disk = template.createElement(DISK_TAG);
		Element image = template.createElement(IMAGE_TAG);
		CDATASection cdata = template.createCDATASection(name);

		image.appendChild(cdata);
		disk.appendChild(image);
		tempNode.insertBefore(disk, firstDisk);
	}

	void setPublicKey(String key) {
		Element e = template.getDocumentElement();
		NodeList nl = e.getElementsByTagName(CONTEXT_TAG);

		Element pub = template.createElement(PUBKEY_TAG);
		CDATASection cdata = template.createCDATASection(key);

		pub.appendChild(cdata);

		if (e.getElementsByTagName(PUBKEY_TAG).getLength() > 0) {
			Node oldpub = e.getElementsByTagName(PUBKEY_TAG).item(0);
			nl.item(0).replaceChild(pub, oldpub);
		} else {
			nl.item(0).appendChild(pub);
		}

	}

	// void setWorkingDir(String dir) {
	// setContextElement(WDIR_TAG, dir);
	// }

	// void setHomeSize(int homeSize) {
	// Element e = template.getDocumentElement();
	// NodeList disks = e.getElementsByTagName(DISK_TAG);
	//
	// for (int i = 0; i < disks.getLength(); i++) {
	// Element disk = (Element) disks.item(i);
	//
	// if (disk.getElementsByTagName(TARGET_TAG).item(0) != null) {
	// String target =
	// disk.getElementsByTagName(TARGET_TAG).item(0).getTextContent();
	//
	// if (target.equals(getHomeDevice())) {
	// Node size = disk.getElementsByTagName(SIZE_TAG).item(0);
	// size.setTextContent(String.valueOf(homeSize));
	// break;
	// }
	// }
	// }
	// }

	// private void setContextElement(String name, String value) {
	// Element e = template.getDocumentElement();
	// NodeList nl = e.getElementsByTagName(name);
	//
	// if (nl.getLength() > 0) {
	// nl.item(0).setTextContent(value);
	// } else {
	// Element context = (Element) e.getElementsByTagName(CONTEXT_TAG).item(0);
	//
	// Element element = template.createElement(name);
	// element.setTextContent(value);
	// context.appendChild(element);
	// }
	// }
	//
	// private void setElement(String name, String value) {
	// Element e = template.getDocumentElement();
	// NodeList nl = e.getElementsByTagName(name);
	//
	// if (nl.getLength() > 0) {
	// Node node = nl.item(0);
	// node.setTextContent(value);
	// }
	// }

	public static void main(String args[]) throws Exception {
		/*
		 * VMTemplate template = new VMTemplate(FileUtils.getStringFromFile(new File("a.xml")));
		 * 
		 * template.setImage("sasaa"); template.setPublicKey("1234");
		 * 
		 * System.out.println(template.getString());
		 */
	}
	
}
