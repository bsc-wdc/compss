/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fullgraph;

import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullgraph.FullGraphScheduler;
import es.bsc.compss.scheduler.fullgraph.utils.Verifiers;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplDefinition;
import es.bsc.compss.scheduler.types.fake.FakeImplementation;
import es.bsc.compss.scheduler.types.fake.FakeProfile;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeResourceScheduler;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class InitialSchedulingTest {

    private static FullGraphScheduler ds;
    private static FakeActionOrchestrator fao;
    private static FakeResourceScheduler drs;

    private static long CORE0;
    private static long CORE1;
    private static long CORE2;


    /**
     * Tests the initial scheduling.
     */
    public InitialSchedulingTest() {
        ds = new FullGraphScheduler();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    /**
     * To setup the class.
     */
    @BeforeClass
    public static void setUpClass() {
        ResourceManager.clear(null);

        CoreManager.clear();

        CoreElementDefinition ced;
        FakeImplDefinition fid;

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature00");
        fid = new FakeImplDefinition("fakeSignature00", new FakeResourceDescription(2));
        ced.addImplementation(fid);
        CoreElement ce0 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl00 = ce0.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature10");
        fid = new FakeImplDefinition("fakeSignature10", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreManager.registerNewCoreElement(ced);
        CoreElement ce1 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl10 = ce1.getImplementation(0);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature20");
        fid = new FakeImplDefinition("fakeSignature20", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreManager.registerNewCoreElement(ced);
        CoreElement ce2 = CoreManager.registerNewCoreElement(ced);
        final Implementation impl20 = ce2.getImplementation(0);

        int maxSlots = 4;
        FakeResourceDescription frd = new FakeResourceDescription(maxSlots);
        FakeWorker fw = new FakeWorker("worker1", frd, maxSlots);
        drs = new FakeResourceScheduler(fw, null, null, fao, 0);

        drs.profiledExecution(impl00, new FakeProfile(50));
        drs.profiledExecution(impl10, new FakeProfile(50));
        drs.profiledExecution(impl20, new FakeProfile(30));

        CORE0 = drs.getProfile(impl00).getAverageExecutionTime();
        CORE1 = drs.getProfile(impl10).getAverageExecutionTime();
        CORE2 = drs.getProfile(impl20).getAverageExecutionTime();

        // debugConfiguration();
    }

    @AfterClass
    public static void tearDownClass() {
        // Nohting to do
    }

    @Before
    public void setUp() {
        // Nohting to do
    }

    @After
    public void tearDown() {
        // Nohting to do
    }

    @Test
    public void testInitialScheduling() throws BlockedActionException, UnassignedActionException {
        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();
        drs.clear();
        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce0Impls);
        action1.selectExecution(drs, (FakeImplementation) action1.getImplementations()[0]);
        drs.scheduleAction(action1);
        Verifiers.verifyInitialPlan(action1, CORE0);

        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 2, 0, ce0Impls);
        action2.selectExecution(drs, (FakeImplementation) action2.getImplementations()[0]);
        drs.scheduleAction(action2);
        Verifiers.verifyInitialPlan(action2, CORE0);

        CoreElement ce1 = CoreManager.getCore(1);
        List<Implementation> ce1Impls = ce1.getImplementations();
        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, ce1Impls);
        action3.selectExecution(drs, (FakeImplementation) action3.getImplementations()[0]);
        drs.scheduleAction(action3);
        Verifiers.verifyInitialPlan(action3, CORE1, action1, action2);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce0Impls);
        action4.selectExecution(drs, (FakeImplementation) action4.getImplementations()[0]);
        drs.scheduleAction(action4);
        Verifiers.verifyInitialPlan(action4, CORE0, action2, action3);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, ce0Impls);
        action5.selectExecution(drs, (FakeImplementation) action5.getImplementations()[0]);
        drs.scheduleAction(action5);
        Verifiers.verifyInitialPlan(action5, CORE0, action3);

        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, ce1Impls);
        action6.selectExecution(drs, (FakeImplementation) action6.getImplementations()[0]);
        drs.scheduleAction(action6);
        Verifiers.verifyInitialPlan(action6, CORE1, action4, action5);

        CoreElement ce2 = CoreManager.getCore(2);
        List<Implementation> ce2Impls = ce2.getImplementations();
        FakeAllocatableAction action7 = new FakeAllocatableAction(fao, 7, 0, ce2Impls);
        action7.selectExecution(drs, (FakeImplementation) action7.getImplementations()[0]);
        drs.scheduleAction(action7);
        Verifiers.verifyInitialPlan(action7, CORE2, action5);

        FakeAllocatableAction action8 = new FakeAllocatableAction(fao, 8, 0, ce2Impls);
        action8.selectExecution(drs, (FakeImplementation) action8.getImplementations()[0]);
        drs.scheduleAction(action8);
        Verifiers.verifyInitialPlan(action8, CORE2, action7);

        FakeAllocatableAction action9 = new FakeAllocatableAction(fao, 9, 0, ce1Impls);
        action9.selectExecution(drs, (FakeImplementation) action9.getImplementations()[0]);
        drs.scheduleAction(action9);
        Verifiers.verifyInitialPlan(action9, CORE1, action6);

        FakeAllocatableAction action10 = new FakeAllocatableAction(fao, 10, 0, ce0Impls);
        action10.selectExecution(drs, (FakeImplementation) action10.getImplementations()[0]);
        drs.scheduleAction(action10);
        Verifiers.verifyInitialPlan(action10, CORE0, action8, action9);

        FakeAllocatableAction action11 = new FakeAllocatableAction(fao, 11, 0, ce0Impls);
        action11.selectExecution(drs, (FakeImplementation) action11.getImplementations()[0]);
        drs.scheduleAction(action11);
        Verifiers.verifyInitialPlan(action11, CORE0, action9);
    }

}
