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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.orderstrict.fifo.FifoTS;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplDefinition;
import es.bsc.compss.scheduler.types.fake.FakeImplDescription;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;

import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.updates.PerformedIncrease;
import es.bsc.compss.types.resources.updates.PerformedReduction;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;


public class FIFOPrioritySchedulerTest {

    public FIFOPrioritySchedulerTest() {
    }

    /**
     * Sets up the class environment before launching the unit tests.
     */
    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /*
     * Test one action has its own implementation and core element, and it is assigned to a worker
     */
    @Test
    public void testOneAction() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker01";
        FakeResourceDescription description = new FakeResourceDescription(1);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        ts.actionCompleted(action0);
    }

    /*
     * we want to check if the scheduler allocates only four tasks since it has four cores and check that the 5th task
     * is allocated when there is a free resource
     * 
     */
    @Test
    public void testEmbarrassinglyParallel() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature10");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature10", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature1", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);

        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker02";
        FakeResourceDescription description = new FakeResourceDescription(4);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);

        ts.actionCompleted(action1);
        ts.actionCompleted(action0);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
    }

    /*
     * The scheduler has more tasks than resources, and we want to see if the behaviour of the scheduler is the
     * expected, allocate first the tasks with the lowest fakeId
     */
    @Test
    public void testFifoAfterBlocked() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker03";
        FakeResourceDescription description = new FakeResourceDescription(4);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
    }

    /*
     * Test using a 4x4 graph creating 4 chains each one with 4 tasks and the task id is consecutive, i.e. chain 0 has
     * tasks 0, 1, 2 and 3, the same order with the other chains.
     */
    @Test
    public void testFIFOOrderedChains() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker04";
        FakeResourceDescription description = new FakeResourceDescription(2);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator, action0);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator, action1);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator, action2);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator, action4);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator, action5);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator, action6);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator, action8);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator, action9);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce0, ts, validator, action10);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce0, ts, validator);
        FakeAllocatableAction action13 = generateNewAA(fao, 13, ce0, ts, validator, action12);
        FakeAllocatableAction action14 = generateNewAA(fao, 14, ce0, ts, validator, action13);
        FakeAllocatableAction action15 = generateNewAA(fao, 15, ce0, ts, validator, action14);

        ts.actionCompleted(action0);
        ts.actionCompleted(action4);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action5);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
        ts.actionCompleted(action10);
        ts.actionCompleted(action6);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);
        ts.actionCompleted(action7);
        ts.actionCompleted(action13);
        ts.actionCompleted(action14);
        ts.actionCompleted(action15);
    }

    /*
     * Test using a 4x4 graph creating 4 chains each one with 4 tasks and the task id is the previous plus 4, i.e. chain
     * 0 has tasks 0, 4, 8 and 12, the same order with the other chains.
     */
    @Test
    public void testFIFOUnorderedChains() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker05";
        FakeResourceDescription description = new FakeResourceDescription(2);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator, action0);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator, action1);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator, action2);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator, action3);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator, action4);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator, action5);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator, action6);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce0, ts, validator, action7);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce0, ts, validator, action8);
        FakeAllocatableAction action13 = generateNewAA(fao, 13, ce0, ts, validator, action9);
        FakeAllocatableAction action14 = generateNewAA(fao, 14, ce0, ts, validator, action10);
        FakeAllocatableAction action15 = generateNewAA(fao, 15, ce0, ts, validator, action11);

        ts.actionCompleted(action0);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action1);
        ts.actionCompleted(action5);
        ts.actionCompleted(action8);
        ts.actionCompleted(action10);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);
        ts.actionCompleted(action14);
        ts.actionCompleted(action15);
        ts.actionCompleted(action9);
        ts.actionCompleted(action13);
    }

    /*
     * There are a number of tasks, some with different core elements and some with dependencies 3 -> 4 dependency and 5
     * -> 6 dependency Tasks 0, 1, 3, 4, 5, 6, 10 and 11 use 1 cpu Tasks 8 and 9 use 2 cpus Tasks 2, 7 and 12 use 3 cpus
     */
    @Test
    public void testFIFODependencyFIllGaps() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def0 = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid0 =
            new FakeImplDescription(def0, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid0);

        FakeImplDefinition def1 = new FakeImplDefinition("fakeSignature01");
        FakeImplDescription fid1 =
            new FakeImplDescription(def1, "fakeSignature01", false, new FakeResourceDescription(2));
        CoreElement ce1 = addCoreElementToCM("fakeSignature1", fid1);

        FakeImplDefinition def2 = new FakeImplDefinition("fakeSignature02");
        FakeImplDescription fid2 =
            new FakeImplDescription(def2, "fakeSignature02", false, new FakeResourceDescription(3));
        CoreElement ce2 = addCoreElementToCM("fakeSignature2", fid2);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName = "Worker06";
        FakeResourceDescription description = new FakeResourceDescription(3);
        addWorkerToTS(workerName, description, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce2, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator, action3);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator, action5);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce2, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce1, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce1, ts, validator);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce0, ts, validator);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce2, ts, validator);
        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action5);
        ts.actionCompleted(action4);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
        ts.actionCompleted(action10);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);

    }

    /*
     * Generate tasks before starting a resource and checks if they are blocked, and after the resource is added they
     * should be executed without problems
     */
    @Test
    public void testBlockedActions() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce0, ts, validator);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce0, ts, validator);
        FakeAllocatableAction action13 = generateNewAA(fao, 13, ce0, ts, validator);
        FakeAllocatableAction action14 = generateNewAA(fao, 14, ce0, ts, validator);
        FakeAllocatableAction action15 = generateNewAA(fao, 15, ce0, ts, validator);
        FakeAllocatableAction action16 = generateNewAA(fao, 16, ce0, ts, validator);
        FakeAllocatableAction action17 = generateNewAA(fao, 17, ce0, ts, validator);
        FakeAllocatableAction action18 = generateNewAA(fao, 18, ce0, ts, validator);
        FakeAllocatableAction action19 = generateNewAA(fao, 19, ce0, ts, validator);

        String workerName = "Worker07";
        FakeResourceDescription description = new FakeResourceDescription(4);
        addWorkerToTS(workerName, description, ts, fao, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
        ts.actionCompleted(action10);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);
        ts.actionCompleted(action13);
        ts.actionCompleted(action14);
        ts.actionCompleted(action15);
        ts.actionCompleted(action16);
        ts.actionCompleted(action17);
        ts.actionCompleted(action18);
        ts.actionCompleted(action19);
    }

    // Two or more resources

    /*
     * Fill gaps in two resources using different core elements and with or without dependencies. 7 -> 8 dependency and
     * 9 -> 10 dependency Task 1, 2, 5, 6, 7, 8, 9, 10, 14, 15 with 1 cpu. Task 3, 4, 12, 13 with 2 cpus. Task 0, 11, 16
     * with 3 cpus
     */
    @Test
    public void testFillTwoResources() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def0 = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid0 =
            new FakeImplDescription(def0, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid0);

        FakeImplDefinition def1 = new FakeImplDefinition("fakeSignature01");
        FakeImplDescription fid1 =
            new FakeImplDescription(def1, "fakeSignature01", false, new FakeResourceDescription(2));
        CoreElement ce1 = addCoreElementToCM("fakeSignature1", fid1);

        FakeImplDefinition def2 = new FakeImplDefinition("fakeSignature02");
        FakeImplDescription fid2 =
            new FakeImplDescription(def2, "fakeSignature02", false, new FakeResourceDescription(3));
        CoreElement ce2 = addCoreElementToCM("fakeSignature2", fid2);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName0 = "Worker08";
        FakeResourceDescription description0 = new FakeResourceDescription(3);
        addWorkerToTS(workerName0, description0, ts, fao, validator);

        String workerName1 = "Worker09";
        FakeResourceDescription description1 = new FakeResourceDescription(3);
        addWorkerToTS(workerName1, description1, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce2, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce2, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce2, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator, action7);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator, action9);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce2, ts, validator);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce1, ts, validator);
        FakeAllocatableAction action13 = generateNewAA(fao, 13, ce1, ts, validator);
        FakeAllocatableAction action14 = generateNewAA(fao, 14, ce0, ts, validator);
        FakeAllocatableAction action15 = generateNewAA(fao, 15, ce0, ts, validator);
        FakeAllocatableAction action16 = generateNewAA(fao, 16, ce2, ts, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
        ts.actionCompleted(action10);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);
        ts.actionCompleted(action13);
        ts.actionCompleted(action14);
        ts.actionCompleted(action15);
        ts.actionCompleted(action16);
    }

    /*
     * Test using a 4x4 graph creating 4 chains each one with 4 tasks and the task id is consecutive, i.e. chain 0 has
     * tasks 0, 1, 2 and 3, the same order with the other chains. Using two resources
     */
    @Test
    public void testChainResources() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid =
            new FakeImplDescription(def, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName0 = "Worker10";
        FakeResourceDescription description0 = new FakeResourceDescription(1);
        addWorkerToTS(workerName0, description0, ts, fao, validator);

        String workerName1 = "Worker11";
        FakeResourceDescription description1 = new FakeResourceDescription(1);
        addWorkerToTS(workerName1, description1, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator, action0);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator, action1);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator, action2);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator, action4);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator, action5);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator, action6);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator, action8);
        FakeAllocatableAction action10 = generateNewAA(fao, 10, ce0, ts, validator, action9);
        FakeAllocatableAction action11 = generateNewAA(fao, 11, ce0, ts, validator, action10);
        FakeAllocatableAction action12 = generateNewAA(fao, 12, ce0, ts, validator);
        FakeAllocatableAction action13 = generateNewAA(fao, 13, ce0, ts, validator, action12);
        FakeAllocatableAction action14 = generateNewAA(fao, 14, ce0, ts, validator, action13);
        FakeAllocatableAction action15 = generateNewAA(fao, 15, ce0, ts, validator, action14);

        ts.actionCompleted(action0);
        ts.actionCompleted(action4);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action5);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
        ts.actionCompleted(action10);
        ts.actionCompleted(action6);
        ts.actionCompleted(action11);
        ts.actionCompleted(action12);
        ts.actionCompleted(action7);
        ts.actionCompleted(action13);
        ts.actionCompleted(action14);
        ts.actionCompleted(action15);
    }

    /*
     * Test if the scheduler assign tasks to the worker with enough cores to run the action
     */
    @Test
    public void testDifferentResources() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def0 = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid0 =
            new FakeImplDescription(def0, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid0);

        FakeImplDefinition def1 = new FakeImplDefinition("fakeSignature01");
        FakeImplDescription fid1 =
            new FakeImplDescription(def1, "fakeSignature01", false, new FakeResourceDescription(2));
        CoreElement ce1 = addCoreElementToCM("fakeSignature1", fid1);

        FakeImplDefinition def2 = new FakeImplDefinition("fakeSignature02");
        FakeImplDescription fid2 =
            new FakeImplDescription(def2, "fakeSignature02", false, new FakeResourceDescription(3));
        CoreElement ce2 = addCoreElementToCM("fakeSignature2", fid2);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName0 = "Worker12";
        FakeResourceDescription description0 = new FakeResourceDescription(2);
        addWorkerToTS(workerName0, description0, ts, fao, validator);

        String workerName1 = "Worker13";
        FakeResourceDescription description1 = new FakeResourceDescription(3);
        addWorkerToTS(workerName1, description1, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce1, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce2, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce1, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce2, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce2, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce1, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);
        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
    }

    /*
     * Check if a task does not have enough resources it goes to blocked and when we add a compatible resource it
     * executes following the correct order
     */
    @Test
    public void testBlockedResources() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def0 = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid0 =
            new FakeImplDescription(def0, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid0);

        FakeImplDefinition def1 = new FakeImplDefinition("fakeSignature01");
        FakeImplDescription fid1 =
            new FakeImplDescription(def1, "fakeSignature01", false, new FakeResourceDescription(3));
        CoreElement ce1 = addCoreElementToCM("fakeSignature1", fid1);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName0 = "Worker14";
        FakeResourceDescription description0 = new FakeResourceDescription(2);
        addWorkerToTS(workerName0, description0, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce1, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce1, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);

        String workerName1 = "Worker15";
        FakeResourceDescription description1 = new FakeResourceDescription(3);
        addWorkerToTS(workerName1, description1, ts, fao, validator);

        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);

    }

    // Pending resource manager refactor
    // @Test
    public void testDeleteWorker() {
        CoreManager.clear();
        ResourceManager.clear(null);
        SchedulingInformation.clear();

        FakeImplDefinition def0 = new FakeImplDefinition("fakeSignature00");
        FakeImplDescription fid0 =
            new FakeImplDescription(def0, "fakeSignature00", false, new FakeResourceDescription(1));
        CoreElement ce0 = addCoreElementToCM("fakeSignature0", fid0);

        FakeActionOrchestrator fao = new FakeActionOrchestrator();
        TaskScheduler ts = new FifoTS(fao);
        fao.setTaskScheduler(ts);
        FIFOValidator validator = new FIFOValidator(ts);

        String workerName0 = "Worker16";
        FakeResourceDescription description0 = new FakeResourceDescription(2);
        addWorkerToTS(workerName0, description0, ts, fao, validator);

        String workerName1 = "Worker17";
        FakeResourceDescription description1 = new FakeResourceDescription(2);
        addWorkerToTS(workerName1, description1, ts, fao, validator);

        FakeAllocatableAction action0 = generateNewAA(fao, 0, ce0, ts, validator);
        FakeAllocatableAction action1 = generateNewAA(fao, 1, ce0, ts, validator);
        FakeAllocatableAction action2 = generateNewAA(fao, 2, ce0, ts, validator);
        FakeAllocatableAction action3 = generateNewAA(fao, 3, ce0, ts, validator);
        FakeAllocatableAction action4 = generateNewAA(fao, 4, ce0, ts, validator);
        FakeAllocatableAction action5 = generateNewAA(fao, 5, ce0, ts, validator);
        FakeAllocatableAction action6 = generateNewAA(fao, 6, ce0, ts, validator);
        FakeAllocatableAction action7 = generateNewAA(fao, 7, ce0, ts, validator);
        FakeAllocatableAction action8 = generateNewAA(fao, 8, ce0, ts, validator);
        FakeAllocatableAction action9 = generateNewAA(fao, 9, ce0, ts, validator);

        ts.actionCompleted(action0);
        ts.actionCompleted(action1);
        ts.actionCompleted(action2);
        ts.actionCompleted(action3);

        removeWorkerFromTS(workerName1, ts, validator);

        ts.actionCompleted(action4);
        ts.actionCompleted(action5);
        ts.actionCompleted(action6);
        ts.actionCompleted(action7);
        ts.actionCompleted(action8);
        ts.actionCompleted(action9);
    }

    private FakeAllocatableAction generateNewAA(FakeActionOrchestrator fao, int id, CoreElement ce, TaskScheduler ts,
        FIFOValidator v, FakeAllocatableAction... predecessor) {
        FakeAllocatableAction action = new FakeAllocatableAction(fao, id, ce, v);
        for (FakeAllocatableAction aa : predecessor) {
            action.addDataPredecessor(aa);
        }
        v.registerAction(action);
        ts.newAllocatableAction(action);
        v.submittedAction(action);
        return action;
    }

    private void addWorkerToTS(String name, FakeResourceDescription description, TaskScheduler ts,
        FakeActionOrchestrator fao, FIFOValidator v) {
        Worker<FakeResourceDescription> worker = new FakeWorker(name, description, Integer.MAX_VALUE);
        worker.updatedFeatures();
        ResourceUpdate<FakeResourceDescription> ru = new PerformedIncrease<>(worker.getDescription());
        v.addResource(name, description);
        ts.updateWorker(worker, ru);
        fao.waitForResourcesStarted();
    }

    private void removeWorkerFromTS(String name, TaskScheduler ts, FIFOValidator v) {
        Iterator<ResourceScheduler<?>> it = ts.getWorkers().iterator();
        while (it.hasNext()) {
            ResourceScheduler<?> rs = it.next();
            if (rs.getName().compareTo(name) == 0) {
                Worker<FakeResourceDescription> worker = (Worker<FakeResourceDescription>) rs.getResource();
                ResourceUpdate<FakeResourceDescription> ru = new PerformedReduction<>(worker.getDescription());
                ts.updateWorker(worker, ru);
                v.removeResource(name);
            }
        }
    }

    private CoreElement addCoreElementToCM(String signature, ImplementationDescription<?, ?>... descriptions) {
        CoreElementDefinition ced;

        ced = new CoreElementDefinition();
        ced.setCeSignature(signature);
        for (ImplementationDescription<?, ?> fid : descriptions) {
            ced.addImplementation(fid);
        }

        return CoreManager.registerNewCoreElement(ced);
    }
}
