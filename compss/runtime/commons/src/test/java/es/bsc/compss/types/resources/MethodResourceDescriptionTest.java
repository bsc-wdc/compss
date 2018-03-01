package es.bsc.compss.types.resources;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.resources.components.Processor;

public class MethodResourceDescriptionTest {
	
	private static final String constraints1 = "ProcessorArchitecture:${processorArchitecture};ComputingUnits:${cu};"
			+ "processors : [{processorType : GPU, ComputingUnits : 3, processorInternalMemorySize:20}, {processorType : FPGA, ComputingUnits : 4}];"
			+ "WallClockLimit:30;OperatingSystemType:Linux;hostQueues:debug;"
			+ "memorySize:20;ProcessorName:Big;ProcessorSpeed:2.3f;StorageSize:100;AppSoftware:Paraver;";

	private static final String constraints2 = "processors : [{processorType : GPU, ComputingUnits : 1, processorInternalMemorySize:20}, {processorType : CPU, ComputingUnits : 8}];";
    @ClassRule
    public static final EnvironmentVariables ENVIRONMENT_VARIABLES = new EnvironmentVariables();


    @Before
    public void setUp() throws Exception {
        ENVIRONMENT_VARIABLES.set("processorArchitecture", "x86_64");
        ENVIRONMENT_VARIABLES.set("cu", "2");
        ENVIRONMENT_VARIABLES.set("gpus", "3");
    }
			
			
	@Test
	public void testC1() {
		MethodResourceDescription mrd = new MethodResourceDescription(constraints1);
		assertEquals(mrd.getTotalCPUComputingUnits(),2);
		assertEquals(mrd.getTotalGPUComputingUnits(),3);
		assertEquals(mrd.getTotalFPGAComputingUnits(),4);
		assertEquals(mrd.getWallClockLimit(),30);
		assertEquals(mrd.getMemorySize(),20,0);
		assertEquals(mrd.getStorageSize(),100,0);
		assertEquals(mrd.getOperatingSystemType(),"Linux");
		assertEquals(mrd.getHostQueues().get(0),"debug".toUpperCase());
		assertEquals(mrd.getAppSoftware().get(0),"Paraver".toUpperCase());
		
		for (Processor p:mrd.getProcessors()){
			if (p.getType().equals(Constants.CPU_TYPE)){
				assertEquals(p.getComputingUnits(),2);
				assertEquals(p.getName(),"Big");
				assertEquals(p.getArchitecture(), "x86_64");
				assertEquals(p.getSpeed(), 2.3f, 0);
			}else if (p.getType().equals(Constants.GPU_TYPE)){
				assertEquals(p.getComputingUnits(),3);
				assertEquals(p.getInternalMemory(), 20, 0);
			}else if (p.getType().equals(Constants.FPGA_TYPE)){
				assertEquals(p.getComputingUnits(),4);
			}
		}
	}
	
	@Test
	public void testC2() throws Exception {
		MethodResourceDescription mrd = new MethodResourceDescription(constraints2);
		assertEquals(mrd.getTotalCPUComputingUnits(),8);
		assertEquals(mrd.getTotalGPUComputingUnits(),1);
		assertEquals(mrd.getTotalFPGAComputingUnits(),0);
		
		for (Processor p:mrd.getProcessors()){
			if (p.getType().equals(Constants.CPU_TYPE)){
				assertEquals(p.getComputingUnits(),8);
			}else if (p.getType().equals(Constants.GPU_TYPE)){
				assertEquals(p.getComputingUnits(),1);
				assertEquals(p.getInternalMemory(), 20, 0);
			}else {
				throw new Exception("Error processor not defined");
			}
		}
	}

}
