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
package es.bsc.compss.scheduler.fullGraphScheduler;

import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullGraphScheduler.utils.Verifiers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.PriorityActionSet;
import es.bsc.compss.scheduler.types.fake.FakeActionOrchestrator;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeImplementation;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.util.CoreManager;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.fail;


public class PriorityActionSetTest {

    private static FullGraphScheduler ds;
    private static FakeActionOrchestrator fao;


    public PriorityActionSetTest() {
        ds = new FullGraphScheduler();
        fao = new FakeActionOrchestrator(ds);
        ds.setOrchestrator(fao);
    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.registerNewCoreElement("fakeSignature00");
        CoreManager.registerNewCoreElement("fakeSignature10");
        CoreManager.registerNewCoreElement("fakeSignature20");

        FakeImplementation impl00 = new FakeImplementation(0, 0,"fakeSignature00", new FakeResourceDescription(2));
        List<Implementation> impls0 = new LinkedList<>();
        impls0.add(impl00);
        CoreManager.registerNewImplementations(0, impls0);

        FakeImplementation impl10 = new FakeImplementation(1, 0, "fakeSignature10", new FakeResourceDescription(3));
        List<Implementation> impls1 = new LinkedList<>();
        impls1.add(impl10);
        CoreManager.registerNewImplementations(1, impls1);

        FakeImplementation impl20 = new FakeImplementation(2, 0, "fakeSignature20", new FakeResourceDescription(1));
        List<Implementation> impls2 = new LinkedList<>();
        impls2.add(impl20);
        CoreManager.registerNewImplementations(2, impls2);
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

        PriorityQueue<AllocatableAction> peeks;

        FakeAllocatableAction action1 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(0));
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action1);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        FakeAllocatableAction action2 = new FakeAllocatableAction(fao, 1, 0, CoreManager.getCoreImplementations(0));
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
        peeks = pas.peekAll();
        AllocatableAction[] expectedPeeks = new AllocatableAction[] { action1, action3 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        FakeAllocatableAction action4 = new FakeAllocatableAction(fao, 4, 0, CoreManager.getCoreImplementations(1));
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

        FakeAllocatableAction action5 = new FakeAllocatableAction(fao, 5, 0, CoreManager.getCoreImplementations(1));
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setToReschedule(true);
        FakeAllocatableAction action6 = new FakeAllocatableAction(fao, 6, 0, CoreManager.getCoreImplementations(1));
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
