package integratedtoolkit.types.project.test;

import static org.junit.Assert.*;

import integratedtoolkit.types.project.ProjectFile;
import integratedtoolkit.types.project.jaxb.CloudProviderType;
import integratedtoolkit.types.project.jaxb.DataNodeType;
import integratedtoolkit.types.project.jaxb.ImageType;
import integratedtoolkit.types.project.jaxb.ProjectType;
import integratedtoolkit.types.project.jaxb.WorkerType;

import java.io.File;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ProjectFileTest {
	
	private static final File TMP_FILE = new File("tmp_file.xml"); 
	
	@BeforeClass
	public static void beforeClass() throws Exception {
		if (TMP_FILE.exists()){
			TMP_FILE.delete();
		}
	}
	
	
	@Test
	public void projectCreationFromFileTest() throws URISyntaxException,
			JAXBException {

		File f = new File(ProjectFileTest.class.getResource("/project.xml")
				.toURI());
		ProjectFile project =  new ProjectFile(f);
		assertNotNull(project);
		List<Object> nodes = project.getProjectType().getDataNodeOrWorkerOrCloud();
		assertEquals("Should have two workers", nodes.size(), 2);

	}
	
	@Test
	public void projectCreationFromString() throws URISyntaxException, JAXBException {
	
		String str = new String(
				"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
				+ "<Project>\n"
				+ "\t<Worker Name=\"localhost\">\n"
				+ "\t\t<InstallDir>/opt/COMPSs/Runtime/scripts/system/</InstallDir>\n"
				+ "\t\t<WorkingDir>/tmp/</WorkingDir>\n"
				+ "\t\t<User>user</User>\n"
				+ "\t\t<LimitOfTasks>4</LimitOfTasks>\n"
				+ "\t</Worker>\n"
				+ "\t<Worker Name=\"http://bscgrid05.bsc.es:20390/hmmerobj/hmmerobj?wsdl\">\n"
				+ "\t\t<LimitOfTasks>3</LimitOfTasks>\n"
				+ "\t</Worker>\n" + "</Project>");
		ProjectFile project = new ProjectFile(str);
		assertNotNull(project);
		assertEquals("Should have two workers", project.getProjectType()
				.getDataNodeOrWorkerOrCloud().size(), 2);
	}
	
	@Test
	public void projectCreationFromScratchTest() throws URISyntaxException, JAXBException {	
		ProjectFile project = new ProjectFile();
		String proStr = project.getString();
		System.out.println(proStr);
		assertTrue("Doesn't contains project tag", (proStr.contains("<Project>") && proStr.contains("</Project>"))||proStr.contains("<Project/>"));
		ProjectFile project2 = new ProjectFile(proStr);
		assertNotNull(project2.getProjectType());
	}
	
	@Test
	public void projectCreationFromScratchToFileTest() throws URISyntaxException, JAXBException {	
		ProjectFile project = new ProjectFile();
		assertFalse(TMP_FILE.exists());
		project.toFile(TMP_FILE);
		assertTrue(TMP_FILE.exists());
		ProjectFile project2 = new ProjectFile(TMP_FILE);
		assertNotNull(project2.getProjectType());
	}
	
	@Test
	public void cloudCreationTest() throws Exception {	
		ProjectFile project = new ProjectFile();
		assertNotNull(project.getCloud());
		assertTrue("Number of providers should be empty", project.getCloudProviders().isEmpty());
		CloudProviderType cp1 = project.addCloudProvider("cp_1");
		assertNotNull(cp1);
		ImageType img= project.addImageToProvider("cp_1","image_1","user","/working/dir/", "/install/dir/");
		assertEquals(cp1.getImageList().getImage().get(0), img);
		ImageType img_2= project.addImageToProvider("cp_2","image_2","user","/working/dir/", "/install/dir/");
		String proStr = project.getString();
		ProjectFile project2 = new ProjectFile(proStr);
		assertEquals("Number of providers should be 2", project2.getCloudProviders().size(),2);
		assertEquals(project2.getCloudProvider("cp_1").getName(), cp1.getName());
		CloudProviderType cp2 = project2.getCloudProvider("cp_2");
		ImageType img_retrieved = cp2.getImageList().getImage().get(0);
		assertEquals("Image names are different",img_retrieved.getName(),img_2.getName());
		assertEquals("Image users are different",img_retrieved.getUser(),img_2.getUser());
		assertEquals("Image wdir are different",img_retrieved.getWorkingDir(),img_2.getWorkingDir());
		assertEquals("Image install dir are different",img_retrieved.getInstallDir(),img_2.getInstallDir());
	}
	
	@Test
	public void workerTest() throws Exception {	
		ProjectFile project = new ProjectFile();
		assertTrue("Number of workers should be empty", project.getWorkers().isEmpty());
		WorkerType worker = project.addWorker("worker_1", "/app/dir/", "/install/dir/", "/working/dir/","user", new Integer(2));
		WorkerType service_worker = project.addWorker("service_worker", null, null, null, null, new Integer(2));
		String proStr = project.getString();
		ProjectFile project2 = new ProjectFile(proStr);
		assertEquals("Number of providers should be 2", project2.getWorkers().size(),2);
		assertEquals(project2.getWorker("service_worker").getName(), service_worker.getName());
		WorkerType worker_retrieved = project2.getWorker("worker_1");
		assertEquals("worker names are different",worker_retrieved.getName(),worker.getName());
		assertEquals("Worker users are different",worker_retrieved.getUser(),worker.getUser());
		assertEquals("Worker wdir are different",worker_retrieved.getWorkingDir(),worker.getWorkingDir());
		assertEquals("Worker app dir are different",worker_retrieved.getAppDir(),worker.getAppDir());
		assertEquals("Worker install dir are different",worker_retrieved.getInstallDir(),worker.getInstallDir());
		assertEquals("Worker limitOfTasks are differetn", worker_retrieved.getLimitOfTasks(),worker.getLimitOfTasks());
	}
	
	@Test
	public void dataNodeTest() throws Exception {	
		ProjectFile project = new ProjectFile();
		assertTrue("Number of workers should be empty", project.getDataNodes().isEmpty());
		DataNodeType dn = project.addDataNode("host", "data/node/path/", "protocol", "user");
		String proStr = project.getString();
		ProjectFile project2 = new ProjectFile(proStr);
		assertEquals("Number of datanode should be 1", project2.getDataNodes().size(),1);
		DataNodeType dn_retrieved = project2.getDataNode("host");
		assertEquals("DN hosts are different",dn_retrieved.getHost(),dn.getHost());
		assertEquals("DN users are different",dn_retrieved.getUser(),dn.getUser());
		assertEquals("DN paths are different",dn_retrieved.getPath(),dn.getPath());
		assertEquals("DN protocols are different",dn_retrieved.getProtocol(),dn.getProtocol());
	}
	
	
	
	@AfterClass
	public static void afterClass() throws Exception {
		if (TMP_FILE.exists()){
			TMP_FILE.delete();
		}
	}
	
	
	
}
