package integratedtoolkit.api.impl;

import static org.junit.Assert.*;
import integratedtoolkit.ITConstants;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.util.CoreManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class COMPSsRuntimeImplTest {
	COMPSsRuntimeImpl rt;
	@Before
	public void setUp() throws Exception {
		rt = new COMPSsRuntimeImpl();
		String resources = this.getClass().getResource("default_resources.xml").getPath();
		String resources_xsd = this.getClass().getResource("resources_schema.xsd").getPath();
		String project = this.getClass().getResource("default_project.xml").getPath();
		String project_xsd = this.getClass().getResource("project_schema.xsd").getPath();
		System.setProperty(ITConstants.IT_LANG, "python");
		System.setProperty(ITConstants.IT_RES_FILE, resources);
		System.setProperty(ITConstants.IT_RES_SCHEMA, resources_xsd);
		System.setProperty(ITConstants.IT_PROJ_FILE, project);
		System.setProperty(ITConstants.IT_PROJ_SCHEMA, project_xsd);
		System.setProperty(ITConstants.COMM_ADAPTOR, "integratedtoolkit.gat.master.GATAdaptor");
		rt.startIT();
	}

	@Test
	public void testPythonCERegister() {
		rt.registerCE("MethodClass", "MethodName", false, false, "ComputingUnits:2", 0, new Object[0]);
		MethodImplementation mi = (MethodImplementation)CoreManager.getCoreImplementations(0)[0];
		assertEquals(2, mi.getRequirements().getProcessors().get(0).getComputingUnits());
	}
	
	@After
	public void tearDown(){
		//rt.stopIT(true);
	}

}
