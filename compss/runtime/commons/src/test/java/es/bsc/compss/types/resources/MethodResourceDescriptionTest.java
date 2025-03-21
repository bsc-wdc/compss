/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.resources;

import static org.junit.Assert.assertEquals;

import es.bsc.compss.types.resources.components.Processor;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;


public class MethodResourceDescriptionTest {

    private static final String constraints1 = "ProcessorArchitecture:${processorArchitecture};ComputingUnits:${cu};"
        + "processors : [" + "{processorType : GPU, ComputingUnits : 3, processorInternalMemorySize:20}, "
        + "{processorType : FPGA, ComputingUnits : 4}" + "];"
        + "WallClockLimit:30;OperatingSystemType:Linux;hostQueues:debug;"
        + "memorySize:20;ProcessorName:Big;ProcessorSpeed:2.3f;StorageSize:100;AppSoftware:Paraver;";

    private static final String constraints2 =
        "processors : [" + "{processorType : GPU, ComputingUnits : 1, processorInternalMemorySize:20},"
            + "{processorType : CPU, ComputingUnits : 8}];";

    @ClassRule
    public static final EnvironmentVariables ENVIRONMENT_VARIABLES = new EnvironmentVariables();


    /**
     * Setup environment before tests execution.
     * 
     * @throws Exception Error when environment cannot be setup.
     */
    @Before
    public void setUp() throws Exception {
        ENVIRONMENT_VARIABLES.set("processorArchitecture", "x86_64");
        ENVIRONMENT_VARIABLES.set("cu", "2");
        ENVIRONMENT_VARIABLES.set("gpus", "3");
    }

    @Test
    public void testC1() {
        MethodResourceDescription mrd = new MethodResourceDescription(constraints1);
        assertEquals(mrd.getTotalCPUComputingUnits(), 2);
        assertEquals(mrd.getTotalGPUComputingUnits(), 3);
        assertEquals(mrd.getTotalFPGAComputingUnits(), 4);
        assertEquals(mrd.getWallClockLimit(), 30);
        assertEquals(mrd.getMemorySize(), 20, 0);
        assertEquals(mrd.getStorageSize(), 100, 0);
        assertEquals(mrd.getOperatingSystemType(), "Linux");
        assertEquals(mrd.getHostQueues().get(0), "debug".toUpperCase());
        assertEquals(mrd.getAppSoftware().get(0), "Paraver".toUpperCase());

        for (Processor p : mrd.getProcessors()) {
            switch (p.getType()) {
                case CPU:
                    assertEquals(p.getComputingUnits(), 2);
                    assertEquals(p.getName(), "Big");
                    assertEquals(p.getArchitecture(), "x86_64");
                    assertEquals(p.getSpeed(), 2.3f, 0);
                    break;
                case GPU:
                    assertEquals(p.getComputingUnits(), 3);
                    assertEquals(p.getInternalMemory(), 20, 0);
                    break;
                case FPGA:
                    assertEquals(p.getComputingUnits(), 4);
                    break;
                default:
                    // Do nothing
                    break;
            }
        }
    }

    @Test
    public void testC2() throws Exception {
        MethodResourceDescription mrd = new MethodResourceDescription(constraints2);
        assertEquals(mrd.getTotalCPUComputingUnits(), 8);
        assertEquals(mrd.getTotalGPUComputingUnits(), 1);
        assertEquals(mrd.getTotalFPGAComputingUnits(), 0);

        for (Processor p : mrd.getProcessors()) {
            switch (p.getType()) {
                case CPU:
                    assertEquals(p.getComputingUnits(), 8);
                    break;
                case GPU:
                    assertEquals(p.getComputingUnits(), 1);
                    assertEquals(p.getInternalMemory(), 20, 0);
                    break;
                default:
                    throw new Exception("Error processor not defined");
            }
        }
    }

}
