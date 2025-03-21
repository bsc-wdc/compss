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
package es.bsc.compss.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.components.Processor.ProcessorType;
import es.bsc.compss.util.parsers.IDLParser;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;


public class IDLParserTest {

    // Test Logger
    private static final Logger LOGGER = LogManager.getLogger("Console");

    private static final int CORECOUNT_RESULT = 7;
    private static final int CORE0_2_3_4_5_IMPLS_RESULT = 1;
    private static final int CORE1_6_IMPLS_RESULT = 3;
    private static final int COMPUTING_UNITS_RESULT = 2;
    private static final int PROCESSOR_COUNT = 2;
    private static final String TEST_2_SIGNATURE =
        "RemoteTrackOnce(" + "INT_T,INT_T,INT_T,INT_T,BINDING_OBJECT_T,BINDING_OBJECT_T,BINDING_OBJECT_T,"
            + "BINDING_OBJECT_T,BINDING_OBJECT_T,INT_T,INT_T,INT_T,INT_T,INT_T,INT_T,BINDING_OBJECT_T)NULL";


    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void loadIDLTest() {
        CoreManager.clear();

        String constraintsFile = this.getClass().getResource("test.idl").getPath();
        List<CoreElementDefinition> ceds = IDLParser.parseIDLMethods(constraintsFile);
        for (CoreElementDefinition ced : ceds) {
            // Create the core Element if it does not exist
            CoreElement ce = CoreManager.registerNewCoreElement(ced);

            Integer coreId = ce.getCoreId();
            String ceSignature = ce.getSignature();
            LOGGER.debug("[IDL-Loader]CoreId for task " + ceSignature + " is " + coreId);
        }
        assertEquals(CoreManager.getCoreCount(), CORECOUNT_RESULT);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 0");
        List<Implementation> implList = CoreManager.getCore(0).getImplementations();
        assertNotNull(implList);
        AbstractMethodImplementation impl = (AbstractMethodImplementation) implList.get(0);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_5_IMPLS_RESULT);
        Processor p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), COMPUTING_UNITS_RESULT);
        assertEquals(p.getArchitecture(), "x86_64");

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 1");
        implList = CoreManager.getCore(1).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (3)");
        assertEquals(implList.size(), CORE1_6_IMPLS_RESULT);
        impl = (AbstractMethodImplementation) implList.get(0);
        assertEquals(impl.getRequirements().getMemorySize(), 2.0f, 0);
        assertEquals(impl.getRequirements().getStorageSize(), 10.0f, 0);
        impl = (AbstractMethodImplementation) implList.get(1);
        p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), COMPUTING_UNITS_RESULT);
        assertEquals(impl.getRequirements().getMemorySize(), 4.0f, 0);
        impl = (AbstractMethodImplementation) implList.get(2);
        p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getComputingUnits(), 1);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 2");
        implList = CoreManager.getCore(2).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_5_IMPLS_RESULT);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 3");
        implList = CoreManager.getCore(3).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_5_IMPLS_RESULT);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 4");
        implList = CoreManager.getCore(4).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_5_IMPLS_RESULT);
        impl = (AbstractMethodImplementation) implList.get(0);
        LOGGER.debug("[IDL-Loader]: Checking Number of processors (2)");
        assertEquals(impl.getRequirements().getProcessors().size(), PROCESSOR_COUNT);
        Processor p1 = impl.getRequirements().getProcessors().get(0);
        LOGGER.debug("[IDL-Loader]: Checking Processor 1 parameters (4)");
        assertEquals(p1.getType(), ProcessorType.CPU);
        assertEquals(p1.getComputingUnits(), 2);
        assertEquals(p1.getArchitecture(), "x86_64");
        assertEquals(p1.getInternalMemory(), 0.6f, 0);

        LOGGER.debug("[IDL-Loader]: Checking Processor 2 parameters (4)");
        Processor p2 = impl.getRequirements().getProcessors().get(1);
        assertEquals(p2.getType(), ProcessorType.GPU);
        assertEquals(p2.getComputingUnits(), 256);
        assertEquals(p2.getArchitecture(), "k40");
        assertEquals(p2.getInternalMemory(), 0.024f, 0);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 5");
        implList = CoreManager.getCore(5).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (1)");
        assertEquals(implList.size(), CORE0_2_3_4_5_IMPLS_RESULT);
        impl = (AbstractMethodImplementation) implList.get(0);
        LOGGER.debug("[IDL-Loader]: Checking Number of processors (1)");
        assertEquals(impl.getRequirements().getProcessors().size(), 1);
        p = impl.getRequirements().getProcessors().get(0);
        LOGGER.debug("[IDL-Loader]: Checking Processor parameters (2)");
        assertEquals(p.getType(), ProcessorType.CPU);
        assertEquals(p.getComputingUnits(), 2);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 6");
        implList = CoreManager.getCore(6).getImplementations();
        assertNotNull(implList);
        LOGGER.debug("[IDL-Loader]: Checking Number of implementations (3)");
        assertEquals(implList.size(), CORE1_6_IMPLS_RESULT);

        impl = (AbstractMethodImplementation) implList.get(0);
        LOGGER.debug("[IDL-Loader]: Checking Number of first implementation processors (1)");
        assertEquals(impl.getRequirements().getProcessors().size(), 1);
        p = impl.getRequirements().getProcessors().get(0);
        assertEquals(p.getType(), ProcessorType.CPU);
        assertEquals(p.getComputingUnits(), 4);

        impl = (AbstractMethodImplementation) implList.get(1);
        LOGGER.debug("[IDL-Loader]: Checking Number of second implementation processors (2)");
        assertEquals(impl.getRequirements().getProcessors().size(), PROCESSOR_COUNT);
        p1 = impl.getRequirements().getProcessors().get(0);
        p2 = impl.getRequirements().getProcessors().get(1);
        LOGGER.debug("[IDL-Loader]: Checking Processor 1 parameters (4)");
        assertEquals(p1.getType(), ProcessorType.CPU);
        assertEquals(p1.getComputingUnits(), 2);
        LOGGER.debug("[IDL-Loader]: Checking Processor 2 parameters (4)");
        assertEquals(p2.getType(), ProcessorType.CPU);
        assertEquals(p2.getComputingUnits(), 2);

        impl = (AbstractMethodImplementation) implList.get(2);
        LOGGER.debug("[IDL-Loader]: Checking Number of third implementation processors (2)");
        assertEquals(impl.getRequirements().getProcessors().size(), PROCESSOR_COUNT);
        p1 = impl.getRequirements().getProcessors().get(0);
        p2 = impl.getRequirements().getProcessors().get(1);
        LOGGER.debug("[IDL-Loader]: Checking Processor 1 parameters (4)");
        assertEquals(p1.getType(), ProcessorType.CPU);
        assertEquals(p1.getComputingUnits(), 1);
        LOGGER.debug("[IDL-Loader]: Checking Processor 2 parameters (4)");
        assertEquals(p2.getType(), ProcessorType.GPU);
        assertEquals(p2.getComputingUnits(), 1);
    }

    @Test
    public void classIDLTest() {
        CoreManager.clear();
        String constraintsFile = this.getClass().getResource("class_test.idl").getPath();
        List<CoreElementDefinition> ceds = IDLParser.parseIDLMethods(constraintsFile);
        for (CoreElementDefinition ced : ceds) {
            // Create the core Element if it does not exist
            CoreElement ce = CoreManager.registerNewCoreElement(ced);

            Integer coreId = ce.getCoreId();
            String ceSignature = ce.getSignature();
            LOGGER.debug("[IDL-Loader] CoreId for task " + ceSignature + " is " + coreId);
        }
        assertEquals(CoreManager.getCoreCount(), 3);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 0");
        List<Implementation> implList = CoreManager.getCore(0).getImplementations();
        assertNotNull(implList);
        assertEquals(implList.size(), 1);

        MethodDefinition impl = (MethodDefinition) implList.get(0).getDescription().getDefinition();
        LOGGER.debug(impl.getDeclaringClass());
        assertEquals(impl.getDeclaringClass(), "Block");

    }

    @Test
    public void test2IDL() {
        CoreManager.clear();

        String constraintsFile = this.getClass().getResource("test_2.idl").getPath();
        List<CoreElementDefinition> ceds = IDLParser.parseIDLMethods(constraintsFile);
        for (CoreElementDefinition ced : ceds) {
            // Create the core Element if it does not exist
            CoreElement ce = CoreManager.registerNewCoreElement(ced);

            Integer coreId = ce.getCoreId();
            String ceSignature = ce.getSignature();
            LOGGER.debug("[IDL-Loader] CoreId for task " + ceSignature + " is " + coreId);
        }
        assertEquals(CoreManager.getCoreCount(), 1);

        LOGGER.debug("[IDL-Loader]: *** Checking Core Element 0");
        List<Implementation> implList = CoreManager.getCore(0).getImplementations();
        assertNotNull(implList);
        assertEquals(implList.size(), 1);

        ImplementationDescription impl = (ImplementationDescription) implList.get(0).getDescription();
        LOGGER.debug(impl.getSignature());
        String signature = impl.getSignature();
        assertEquals(signature, TEST_2_SIGNATURE);
    }

}
