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

import static org.junit.Assert.fail;

import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullgraph.FullGraphScheduler;
import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.fullgraph.utils.Verifiers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.PriorityActionSet;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplDefinition;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class PriorityActionSetTest {

    private static FullGraphScheduler ds;
    private static FakeActionOrchestrator fao;


    /**
     * Tests the PriorityActionSet.
     */
    public PriorityActionSetTest() {
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

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature10");
        fid = new FakeImplDefinition("fakeSignature10", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreManager.registerNewCoreElement(ced);

        ced = new CoreElementDefinition();
        ced.setCeSignature("fakeSignature20");
        fid = new FakeImplDefinition("fakeSignature20", new FakeResourceDescription(3));
        ced.addImplementation(fid);
        CoreManager.registerNewCoreElement(ced);
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

    @Test
    public void testInitialScheduling() throws BlockedActionException, UnassignedActionException {
        PriorityActionSet pas = new PriorityActionSet(new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction o1, AllocatableAction o2) {
                return Long.compare(o1.getId(), o2.getId());
            }
        });

        CoreElement ce0 = CoreManager.getCore(0);
        List<Implementation> ce0Impls = ce0.getImplementations();

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, ce0Impls);
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action1);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 1, 0, ce0Impls);
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action2);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        FakeAllocatableAction action3 = new FakeAllocatableAction(fao, 3, 0, null);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action3);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        PriorityQueue<AllocatableAction> peeks = pas.peekAll();
        AllocatableAction[] expectedPeeks = new AllocatableAction[] { action1, action3 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        CoreElement ce1 = CoreManager.getCore(1);
        List<Implementation> ce1Impls = ce1.getImplementations();

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, ce1Impls);
        ((FullGraphSchedulingInformation) action4.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action4);
        peeks = pas.peekAll();
        expectedPeeks = new AllocatableAction[] { action1, action3, action4 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        AllocatableAction action = pas.poll();
        if (action1 != action) {
            fail(action1 + " expected to be the most prioritary action and " + action + " was.");
        }
        peeks = pas.peekAll();
        expectedPeeks = new AllocatableAction[] { action2, action3, action4 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        action = pas.poll();
        if (action2 != action) {
            fail(action2 + " expected to be the most prioritary action and " + action + " was.");
        }
        peeks = pas.peekAll();
        expectedPeeks = new AllocatableAction[] { action3, action4 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        action = pas.poll();
        if (action3 != action) {
            fail(action3 + " expected to be the most prioritary action and " + action + " was.");
        }
        peeks = pas.peekAll();
        expectedPeeks = new AllocatableAction[] { action4 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        action = pas.poll();
        if (action4 != action) {
            fail(action4 + " expected to be the most prioritary action and " + action + " was.");
        }
        peeks = pas.peekAll();
        expectedPeeks = new AllocatableAction[] {};
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, ce1Impls);
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setToReschedule(true);
        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, ce1Impls);
        ((FullGraphSchedulingInformation) action6.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action6);
        action = pas.peek();
        if (action6 != action) {
            fail(action6 + " expected to be the most prioritary action and " + action + " was.");
        }
        pas.offer(action5);
        action = pas.peek();
        if (action5 != action) {
            fail(action5 + " expected to be the most prioritary action and " + action + " was.");
        }
    }

}
