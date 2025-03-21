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
package es.bsc.compss.test.api.impl;

import static org.junit.Assert.assertEquals;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.util.CoreManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class COMPSsRuntimeImplTest {

    private static final String DUMMY_ADAPTOR_CLASS = "es.bsc.compss.test.dummyadaptor.DummyAdaptor";
    private COMPSsRuntimeImpl rt;

    static {
        System.setProperty(COMPSsConstants.COMM_ADAPTOR, DUMMY_ADAPTOR_CLASS);
    }


    /**
     * Set up the JVM to execute the tests.
     * 
     * @throws Exception When a Runtime error occurs.
     */
    @Before
    public void setUp() throws Exception {
        String resources = this.getClass().getResource("resources.xml").getPath();
        String resourcesXSD = this.getClass().getResource("resources_schema.xsd").getPath();
        String project = this.getClass().getResource("project.xml").getPath();
        String projectXSD = this.getClass().getResource("project_schema.xsd").getPath();
        System.setProperty(COMPSsConstants.LANG, COMPSsConstants.Lang.PYTHON.name());
        System.setProperty(COMPSsConstants.RES_FILE, resources);
        System.setProperty(COMPSsConstants.RES_SCHEMA, resourcesXSD);
        System.setProperty(COMPSsConstants.PROJ_FILE, project);
        System.setProperty(COMPSsConstants.PROJ_SCHEMA, projectXSD);

        this.rt = new COMPSsRuntimeImpl();
        this.rt.startIT();
    }

    @Test
    public void testPythonCERegister() {
        // METHOD
        String coreElementSignature = "methodClass.methodName";
        String implSignature = "methodClass.methodName";
        String implConstraints = "ComputingUnits:2";
        String implType = "METHOD";
        String implLocal = "False";
        String implIO = "False";
        String[] prolog = new String[] { "",
            "",
            "" };
        String[] epilog = new String[] { "",
            "",
            "" };
        String[] container = new String[] { "",
            "",
            "" };
        String[] implTypeArgs = new String[] { "methodClass",
            "methodName" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        CoreElement ce0 = CoreManager.getCore(0);
        AbstractMethodImplementation mi = (AbstractMethodImplementation) ce0.getImplementations().get(0);
        assertEquals(2, mi.getRequirements().getProcessors().get(0).getComputingUnits());

        // MPI
        coreElementSignature = "methodClass1.methodName1";
        implSignature = "mpi.MPI";
        implConstraints = "StorageType:SSD";
        implType = "MPI";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "mpiBinary",
            "mpiWorkingDir",
            "mpiRunner",
            "1", // processes_per_node
            "mpiFlags",
            "false",
            "[unassigned]",
            "false" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(1).getImplementations().get(0);
        assertEquals(MethodType.MPI, mi.getMethodType());
        assertEquals("SSD", mi.getRequirements().getStorageType());

        // DECAF
        coreElementSignature = "methodClass1.methodName1";
        implSignature = "decaf.DECAF";
        implConstraints = "StorageSize:2.0";
        implType = "DECAF";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "dfScript",
            "dfExceutor",
            "dfLib",
            "dfWorkingDir",
            "mpiRunner",
            "false" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(1).getImplementations().get(1);
        assertEquals(MethodType.DECAF, mi.getMethodType());
        assertEquals(2.0, mi.getRequirements().getStorageSize(), 0.1);

        // BINARY
        coreElementSignature = "methodClass2.methodName2";
        implSignature = "binary.BINARY";
        implConstraints = "MemoryType:RAM";
        implType = "BINARY";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "binary",
            "binaryWorkingDir",
            "[unassigned]",
            "false" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(2).getImplementations().get(0);
        assertEquals(MethodType.BINARY, mi.getMethodType());
        assertEquals("RAM", mi.getRequirements().getMemoryType());

        // OMPSS
        coreElementSignature = "methodClass3.methodName3";
        implSignature = "ompss.OMPSS";
        implConstraints = "ComputingUnits:3";
        implType = "OMPSS";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "ompssBinary",
            "ompssWorkingDir",
            "false" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(3).getImplementations().get(0);
        assertEquals(MethodType.OMPSS, mi.getMethodType());
        assertEquals(3, mi.getRequirements().getProcessors().get(0).getComputingUnits());

        // OPENCL
        coreElementSignature = "methodClass4.methodName4";
        implSignature = "opencl.OPENCL";
        implConstraints = "ComputingUnits:4";
        implType = "OPENCL";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "openclKernel",
            "openclWorkingDir" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(4).getImplementations().get(0);
        assertEquals(MethodType.OPENCL, mi.getMethodType());
        assertEquals(4, mi.getRequirements().getProcessors().get(0).getComputingUnits());

        // VERSIONING
        coreElementSignature = "methodClass.methodName";
        implSignature = "anotherClass.anotherMethodName";
        implConstraints = "ComputingUnits:1";
        implType = "METHOD";
        implLocal = "False";
        implIO = "False";
        implTypeArgs = new String[] { "anotherClass",
            "anotherMethodName" };
        rt.registerCoreElement(coreElementSignature, implSignature, implConstraints, implType, implLocal, implIO,
            prolog, epilog, container, implTypeArgs);

        mi = (AbstractMethodImplementation) CoreManager.getCore(0).getImplementations().get(1);
        assertEquals(MethodType.METHOD, mi.getMethodType());
        assertEquals(1, mi.getRequirements().getProcessors().get(0).getComputingUnits());
    }

    @After
    public void tearDown() {
        // rt.stopIT(true);
    }

}
