package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.fullGraphScheduler.utils.Verifiers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.PriorityActionSet;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeImplementation;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.util.CoreManager;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


public class PriorityActionSetTest {

    public PriorityActionSetTest() {

    }

    @BeforeClass
    public static void setUpClass() {
        CoreManager.clear();
        CoreManager.resizeStructures(3);

        Implementation<?> impl00 = new FakeImplementation(0, 0, new FakeResourceDescription(2));
        CoreManager.registerImplementations(0, new Implementation[] { impl00 }, new String[] { "fakeSignature00" });
        Implementation<?> impl10 = new FakeImplementation(1, 0, new FakeResourceDescription(3));
        CoreManager.registerImplementations(1, new Implementation[] { impl10 }, new String[] { "fakeSignature10" });
        Implementation<?> impl20 = new FakeImplementation(2, 0, new FakeResourceDescription(1));
        CoreManager.registerImplementations(2, new Implementation[] { impl20 }, new String[] { "fakeSignature20" });
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

        FakeAllocatableAction action1 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(0));
        ((FullGraphSchedulingInformation) action1.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action1);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        FakeAllocatableAction action2 = new FakeAllocatableAction(1, 0, CoreManager.getCoreImplementations(0));
        ((FullGraphSchedulingInformation) action2.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action2);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        FakeAllocatableAction action3 = new FakeAllocatableAction(3, 0, null);
        ((FullGraphSchedulingInformation) action3.getSchedulingInfo()).setToReschedule(true);
        pas.offer(action3);
        if (action1 != pas.peek()) {
            fail(action1 + " expected to be the most prioritary action and " + pas.peek() + " was.");
        }
        peeks = pas.peekAll();
        AllocatableAction[] expectedPeeks = new AllocatableAction[] { action1, action3 };
        Verifiers.verifyPriorityActions(peeks, expectedPeeks);

        FakeAllocatableAction action4 = new FakeAllocatableAction(4, 0, CoreManager.getCoreImplementations(1));
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

        FakeAllocatableAction action5 = new FakeAllocatableAction(5, 0, CoreManager.getCoreImplementations(1));
        ((FullGraphSchedulingInformation) action5.getSchedulingInfo()).setToReschedule(true);
        FakeAllocatableAction action6 = new FakeAllocatableAction(6, 0, CoreManager.getCoreImplementations(1));
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
