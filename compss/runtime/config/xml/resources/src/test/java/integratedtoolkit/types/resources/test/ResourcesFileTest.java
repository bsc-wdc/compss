package integratedtoolkit.types.resources.test;

import static org.junit.Assert.*;

import integratedtoolkit.types.resources.ResourcesFile;
import integratedtoolkit.types.resources.jaxb.CloudType;
import integratedtoolkit.types.resources.jaxb.DataNodeType;
import integratedtoolkit.types.resources.jaxb.DiskType;
import integratedtoolkit.types.resources.jaxb.ImageType;
import integratedtoolkit.types.resources.jaxb.ResourceTypeWithPorts;
import integratedtoolkit.types.resources.jaxb.ServiceType;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


public class ResourcesFileTest {
	
	private static final File TMP_FILE = new File("tmp_file.xml"); 
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		if (TMP_FILE.exists()){
			TMP_FILE.delete();
		}
	}
	
	
	@Test
	public void resourcesCreationFromFileTest() throws URISyntaxException,
			JAXBException {

		File f = new File(ResourcesFileTest.class.getResource("/resources.xml")
				.toURI());
		ResourcesFile resources =  new ResourcesFile(f);
		assertNotNull(resources);
		List<Object> nodes = resources.getResourceListType().getDiskOrDataNodeOrResource();
		assertEquals("Should have two workers", nodes.size(), 2);

	}
	
	@Test
	public void resourcesFileCreationFromString() throws URISyntaxException,
			JAXBException {

		String str = new String("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<ResourceList>\n" + "\t<Resource Name=\"localhost\">\n"
				+ "\t\t<Capabilities>\n" + "\t\t\t<Host>\n"
				+ "\t\t\t\t<TaskCount>0</TaskCount>\n"
				+ "\t\t\t\t<Queue>short</Queue>\n" + "\t\t\t\t<Queue/>\n"
				+ "\t\t\t</Host>\n" + "\t\t\t<Processor>\n"
				+ "\t\t\t\t<Architecture>IA32</Architecture>\n"
				+ "\t\t\t\t<Speed>3.0</Speed>\n"
				+ "\t\t\t\t<CPUCount>4</CPUCount>\n" + "\t\t\t</Processor>\n"
				+ "\t\t\t<OS>\n" + "\t\t\t\t<OSType>Linux</OSType>\n"
				+ "\t\t\t\t<MaxProcessesPerUser>32</MaxProcessesPerUser>\n"
				+ "\t\t\t</OS>\n" + "\t\t\t<StorageElement>\n"
				+ "\t\t\t\t<Size>8</Size>\n" + "\t\t\t</StorageElement>\n"
				+ "\t\t\t<Memory>\n"
				+ "\t\t\t\t<PhysicalSize>4</PhysicalSize>\n"
				+ "\t\t\t\t<VirtualSize>8</VirtualSize>\n"
				+ "\t\t\t</Memory>\n" + "\t\t\t<ApplicationSoftware>\n"
				+ "\t\t\t\t<Software>Java</Software>\n"
				+ "\t\t\t</ApplicationSoftware>\n" + "\t\t\t<Service/>\n"
				+ "\t\t\t<VO/>\n" + "\t\t\t<Cluster/>\n"
				+ "\t\t\t<FileSystem/>\n" + "\t\t\t<NetworkAdaptor/>\n"
				+ "\t\t\t<JobPolicy/>\n" + "\t\t\t<AccessControlPolicy/>\n"
				+ "\t\t</Capabilities>\n" + "\t\t<Requirements/>\n"
				+ "\t </Resource>\n" + "</ResourceList>");
		ResourcesFile resources = new ResourcesFile(str);
		assertNotNull(resources);
		assertEquals("Should have a resource", resources.getResourceListType()
				.getDiskOrDataNodeOrResource().size(), 1);
	}
	
	@Test
	public void resourcesFileCreationFromScratchTest() throws URISyntaxException, JAXBException {	
		ResourcesFile resources = new ResourcesFile();
		String proStr = resources.getString();
		System.out.println(proStr);
		assertTrue("Doesn't contains resources tag", (proStr.contains("<ResourceList>") && proStr.contains("</ResourceList>"))||proStr.contains("<ResourceList/>"));
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertNotNull(resources2.getResourceListType());
	}
	
	@Test
	public void resourcesCreationFromScratchToFileTest() throws URISyntaxException, JAXBException {	
		ResourcesFile resources = new ResourcesFile();
		assertFalse(TMP_FILE.exists());
		resources.toFile(TMP_FILE);
		assertTrue(TMP_FILE.exists());
		ResourcesFile resources2 = new ResourcesFile(TMP_FILE);
		assertNotNull(resources2.getResourceListType());
	}
	
	@Test
	public void cloudCreationTest() throws Exception {	
		ResourcesFile resources = new ResourcesFile();
		
		assertTrue("Number of providers should be empty", resources.getCloudProviders().isEmpty());
		CloudType cp1 = resources.addCloudProvider("cp_1");
		assertNotNull(cp1);
		ImageType img= resources.addImageToCloudProvider("cp_1","image_1",null);
		assertEquals(cp1.getImageList().getImage().get(0), img);
		ImageType img_2= resources.addImageToCloudProvider("cp_2","image_2",null);
		String proStr = resources.getString();
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertEquals("Number of providers should be 2", resources2.getCloudProviders().size(),2);
		assertEquals(resources2.getCloudProvider("cp_1").getName(), cp1.getName());
		CloudType cp2 = resources2.getCloudProvider("cp_2");
		ImageType img_retrieved = cp2.getImageList().getImage().get(0);
		assertEquals("Image names are different",img_retrieved.getName(),img_2.getName());
	}
	
	@Test
	public void resourceCreationTest() throws Exception {	
		ResourcesFile resources = new ResourcesFile();
		assertTrue("Number of workers should be empty", resources.getResources().isEmpty());
		ResourceTypeWithPorts worker = resources.addResource("res_1", "x86", new Float(2.5), new Integer(2),
				new Integer(3), "Linux", new Float(2.5), new Float(2.5), new String[]{"Java"},
				new Integer(0), new String[]{"short"});
		System.out.println("Resources:"+resources.getString());
		assertNotNull("Resource object is null",worker);
		assertNotNull("Resource capabilites object is null", worker.getCapabilities());
		assertNotNull("Resource processor object is null", worker.getCapabilities().getProcessor());
		assertNotNull("Resource OS object is null", worker.getCapabilities().getOS());
		assertNotNull("Resource Memory object is null", worker.getCapabilities().getMemory());
		assertNotNull("Resource Storage object is null", worker.getCapabilities().getStorageElement());
		assertNotNull("Resource Host object is null", worker.getCapabilities().getHost());
		String proStr = resources.getString();
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertEquals("Number of providers should be 1", resources2.getResources().size(),1);
		ResourceTypeWithPorts worker_retrieved = resources2.getResource("res_1");
		assertEquals("worker names are different",worker_retrieved.getName(),worker.getName());
		assertEquals("Resource architecture are different",worker_retrieved.getCapabilities().getProcessor().getArchitecture(),
				worker.getCapabilities().getProcessor().getArchitecture());
	}
	
	@Test
	public void serviceCreationTest() throws Exception {	
		ResourcesFile resources = new ResourcesFile();
		assertTrue("Number of workers should be empty", resources.getServices().isEmpty());
		ServiceType worker = resources.addService("serviceName", "namespace", "port", "wsdlLoc");
		String proStr = resources.getString();
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertEquals("Number of providers should be 1", resources2.getServices().size(),1);
		ServiceType worker_retrieved = resources2.getService("wsdlLoc");
		assertEquals("Service names are different",worker_retrieved.getName(),worker.getName());
		assertEquals("Service namespaces are different",worker_retrieved.getNamespace(),
				worker.getNamespace());
	}
	
	@Test
	public void dataNodeTest() throws Exception {	
		ResourcesFile resources = new ResourcesFile();
		assertTrue("Number of workers should be empty", resources.getDataNodes().isEmpty());
		DataNodeType dn = resources.addDataNode("host", "data/node/path/");
		String proStr = resources.getString();
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertEquals("Number of datanode should be 1", resources2.getDataNodes().size(),1);
		DataNodeType dn_retrieved = resources2.getDataNode("host");
		assertEquals("DN hosts are different",dn_retrieved.getHost(),dn.getHost());
		assertEquals("DN paths are different",dn_retrieved.getPath(),dn.getPath());
	}
	
	@Test
	public void diskTest() throws Exception {	
		ResourcesFile resources = new ResourcesFile();
		assertTrue("Number of workers should be empty", resources.getDataNodes().isEmpty());
		DiskType d1 = resources.addDisk("shared", "/mount/path/");
		assertEquals("Number of disks should be 1", resources.getDisks().size(),1);
		DiskType d3 = resources.addDisk("shared", "/mount/path/new");
		assertEquals("Number of disks should be 1", resources.getDisks().size(),1);
		assertEquals("Mount path are different",d1.getMountPoint(),"/mount/path/new");
		DiskType d2 = resources.addDisk("shared_2", "/mount/path/");
		ResourceTypeWithPorts worker = resources.addResource("res_1", "x86", new Float(2.5), new Integer(2),
				new Integer(3), "Linux", new Float(2.5), new Float(2.5), new String[]{"Java"}, 
				new Integer(0), new String[]{"short"});
		ResourcesFile.addDiskToResource("shared", "/mount/path/res_1/shared", worker);
		ResourcesFile.addDiskToResource("shared_2", "/mount/path/res_1/shared_2", worker);
		String proStr = resources.getString();
		ResourcesFile resources2 = new ResourcesFile(proStr);
		assertEquals("Number of disks should be 2", resources2.getDisks().size(),2);
		DiskType d1_retrieved = resources2.getDisk("shared");
		assertEquals("Mount path are different",d1_retrieved.getMountPoint(),d1.getMountPoint());
		ResourceTypeWithPorts worker_retrieved = resources2.getResource("res_1");
		DiskType d2_retrieved = ResourcesFile.getDiskFromResource("shared_2", worker_retrieved);
		assertEquals("Mount path are different",d2_retrieved.getMountPoint(),"/mount/path/res_1/shared_2");
		ResourcesFile.removeDiskFromResource("shared", worker_retrieved);
		ResourcesFile.removeDiskFromResource("shared_2", worker_retrieved);
		boolean exThrown = false;
		try{
			DiskType d = ResourcesFile.getDiskFromResource("shared_2", worker_retrieved);
			if (d==null){
				System.out.print("Disk shared_2 is null");
				exThrown=true;
			}
		}catch (Exception e){
			System.out.print("Exception intercepted");
			exThrown= true;
		}
		assertTrue("Disk has not been removed",exThrown);
		resources2.removeDisk("shared");
		assertEquals("Number of disks after first remove should be 1", resources2.getDisks().size(),1);
		resources2.removeDisk("shared_2");
		assertEquals("Number of disks after first remove should be 0", resources2.getDisks().size(),0);
	}
	
	
	@AfterClass
	public static void afterClass() throws Exception {
		if (TMP_FILE.exists()){
			TMP_FILE.delete();
		}
	}
	
	
	
}
