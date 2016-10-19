package integratedtoolkit.util;

import static org.junit.Assert.*;

import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.components.Processor;

import java.util.HashMap;
import java.util.LinkedList;

import org.junit.Before;
import org.junit.Test;

public class IDLParserTest {

	private static final int CORECOUNT_RESULT=5;
	private static final int CORE0_2_3_4_IMPLS_RESULT=1;
	private static final int CORE1_IMPLS_RESULT=3;
	private static final int COMPUTING_UNITS_RESULT = 2;
	private static final int PROCESSOR_COUNT = 2;
	
	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void loadIDLTest() {
		String constraintsFile = this.getClass().getResource("test.idl").getPath();
		LinkedList<Integer> updatedMethods = new LinkedList<Integer>();
        HashMap<Integer, LinkedList<MethodImplementation>> readMethods = new HashMap<Integer, LinkedList<MethodImplementation>>();
        HashMap<Integer, LinkedList<String>> readSignatures = new HashMap<Integer, LinkedList<String>>();
        int coreCount = IDLParser.parseIDLMethods(updatedMethods, readMethods, readSignatures, constraintsFile);
		assertEquals(coreCount, CORECOUNT_RESULT);
		assertEquals(readMethods.size(), CORECOUNT_RESULT);
		System.out.println("[IDL-Loader]: *** Checking Core Element 0");
        LinkedList<MethodImplementation> implList = readMethods.get(0);
        assertNotNull(implList);
        MethodImplementation impl = implList.get(0);
        System.out.println("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_IMPLS_RESULT);
        Processor p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), COMPUTING_UNITS_RESULT);
        assertEquals(p.getArchitecture(), "x86_64");
        
        System.out.println("[IDL-Loader]: *** Checking Core Element 1");
        implList = readMethods.get(1);
        assertNotNull(implList);
        System.out.println("[IDL-Loader]: Checking Number of implementations (3)");
        assertEquals(implList.size(), CORE1_IMPLS_RESULT);
        impl = implList.get(0);
        assertEquals(impl.getRequirements().getMemorySize(), 2.0f,0);
        assertEquals(impl.getRequirements().getStorageSize(), 10.0f,0);
        impl = implList.get(1);
        p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), COMPUTING_UNITS_RESULT);
        assertEquals(impl.getRequirements().getMemorySize(), 4.0f,0);
        impl = implList.get(2);
        p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), 1);
      
        System.out.println("[IDL-Loader]: *** Checking Core Element 2");
        implList = readMethods.get(2);
        assertNotNull(implList);
        System.out.println("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_IMPLS_RESULT);
        
        System.out.println("[IDL-Loader]: *** Checking Core Element 3");
        implList = readMethods.get(3);
        assertNotNull(implList);
        System.out.println("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_IMPLS_RESULT);
       
        System.out.println("[IDL-Loader]: *** Checking Core Element 4");
        implList = readMethods.get(4);
        assertNotNull(implList);
        System.out.println("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_IMPLS_RESULT);
        impl = implList.get(0);
        System.out.println("[IDL-Loader]: Checking Number of processors (2)");
        assertEquals(impl.getRequirements().getProcessors().size(), PROCESSOR_COUNT);
        Processor p1 = impl.getRequirements().getProcessors().get(0);
        Processor p2 = impl.getRequirements().getProcessors().get(1);
        System.out.println("[IDL-Loader]: Checking Processor 1 parameters (4)");
        assertEquals(p1.getType(), "CPU");
        assertEquals(p1.getComputingUnits(), 2);
        assertEquals(p1.getArchitecture(), "x86_64");
        assertEquals(p1.getInternalMemory(), 0.6f,0);
   
        System.out.println("[IDL-Loader]: Checking Processor 2 parameters (4)");
        assertEquals(p2.getType(), "GPU");
        assertEquals(p2.getComputingUnits(), 256);
        assertEquals(p2.getArchitecture(), "k40");
        assertEquals(p2.getInternalMemory(), 0.024f,0);  
	}

}
