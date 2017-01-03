package integratedtoolkit.scheduler.fullGraphScheduler.utils;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FullGraphScore;
import integratedtoolkit.scheduler.types.Gap;
import integratedtoolkit.scheduler.types.OptimizationWorker;
import integratedtoolkit.scheduler.types.PriorityActionSet;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.scheduler.types.fake.FakeAllocatableAction;
import integratedtoolkit.scheduler.types.fake.FakeResourceDescription;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import static org.junit.Assert.fail;


public class Verifiers {

    public static void verifyPriorityActions(PriorityActionSet obtained, AllocatableAction[] expected) {
        int idx = 0;
        if (obtained.size() != expected.length) {
            fail("Obtained lists doesn't match on size.");
        }
        PriorityQueue<AllocatableAction> obtainedCopy = new PriorityQueue<>();
        while (obtainedCopy.size() > 0) {
            AllocatableAction action = obtainedCopy.poll();
            AllocatableAction expectedAction = expected[idx];
            if (!expectedAction.equals(action)) {
                fail(expectedAction + " expected to be the most prioritary action and " + action + " was.");
            }
            idx++;
        }
    }

    public static void verifyPriorityActions(PriorityQueue<AllocatableAction> obtained, AllocatableAction[] expected) {
        int idx = 0;
        if (obtained.size() != expected.length) {
            fail("Obtained lists doesn't match on size.");
        }
        PriorityQueue<AllocatableAction> obtainedCopy = new PriorityQueue<>();
        while (obtainedCopy.size() > 0) {
            AllocatableAction action = obtainedCopy.poll();
            AllocatableAction expectedAction = expected[idx];
            if (!expectedAction.equals(action)) {
                fail(expectedAction + " expected to be the most prioritary action and " + action + " was.");
            }
            idx++;
        }
    }

    public static void verifyReadyActions(PriorityQueue<AllocatableAction> obtained, HashMap<AllocatableAction, Long> expectedReady) {
        if (obtained.size() != expectedReady.size()) {
            fail("Obtained lists doesn't match on size.");
        }
        long lastTime = 0;
        while (obtained.size() > 0) {
            AllocatableAction action = obtained.poll();
            long start = ((FullGraphSchedulingInformation) action.getSchedulingInfo()).getExpectedStart();
            if (start < lastTime) {
                fail("Ready actions are not ordered according to the time");
            }
            lastTime = start;
            long time = expectedReady.remove(action);
            if (start != time) {
                fail("Ready action " + action + "starts at " + start + " and was supposed to start at " + time);
            }
        }
    }

    public static void verifyWorkersPriority(LinkedList<OptimizationWorker> obtained, LinkedList<String> expected) {
        int idx = 0;
        if (obtained.size() != expected.size()) {
            fail("Obtained lists doesn't match on size.");
        }
        while (obtained.size() > 0) {
            OptimizationWorker donor = obtained.poll();
            String donorName = donor.getName();
            String expectedName = expected.get(idx);
            if (!donorName.equals(expectedName)) {
                fail("Expected worker " + expectedName + " and obtianed " + donorName);
            }
            idx++;
        }
    }

    public static void verifyScore(FullGraphScore ds, long action, long data, long res, long impl, long start) {
        if (action != ds.getActionScore()) {
            System.out.println("Scores do not match. Expected action score " + action + " and " + ds.getActionScore() + " obtianed.");
            fail("Scores do not match. Expected action score " + action + " and " + ds.getActionScore() + " obtianed.");
        }
        if (data != ds.getExpectedDataAvailable()) {
            System.out.println(
                    "Scores do not match. Expected data available at " + data + " and " + ds.getExpectedDataAvailable() + " obtianed.");
            fail("Scores do not match. Expected data available at " + data + " and " + ds.getExpectedDataAvailable() + " obtianed.");
        }
        if (res != ds.getResourceScore()) {
            System.out.println("Scores do not match. Expected resource score " + res + " and " + ds.getResourceScore() + " obtianed.");
            fail("Scores do not match. Expected resource score " + res + " and " + ds.getResourceScore() + " obtianed.");
        }
        if (impl != ds.getImplementationScore()) {
            System.out.println(
                    "Scores do not match. Expected implementation score " + impl + " and " + ds.getImplementationScore() + " obtianed.");
            fail("Scores do not match. Expected implementation score " + impl + " and " + ds.getImplementationScore() + " obtianed.");
        }
        if (start != ds.getExpectedStart()) {
            System.out.println("Scores do not match. Expected start time " + start + " and " + ds.getExpectedStart() + " obtianed.");
            fail("Scores do not match. Expected start time " + start + " and " + ds.getExpectedStart() + " obtianed.");
        }
    }

    public static void validateBetterScore(Score a, Score b, boolean result) {
        if (Score.isBetter(a, b) != result) {
            System.out.println("Scores are not properly compared");
            fail("Scores are not properly compared");
        }
    }

    public static void verifyUpdate(LinkedList<AllocatableAction>[] actions, long[][][] times) {
        for (int coreId = 0; coreId < actions.length; coreId++) {
            for (int actionIdx = 0; actionIdx < actions[coreId].size(); actionIdx++) {
                AllocatableAction action = actions[coreId].get(actionIdx);
                FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
                if (dsi.getExpectedStart() != times[coreId][actionIdx][0] || dsi.getExpectedEnd() != times[coreId][actionIdx][1]) {
                    System.out.println("Action " + action + " has not been updated properly.\n" + "Expected start at "
                            + times[coreId][actionIdx][0] + " and end at " + times[coreId][actionIdx][1] + "\n" + "Obtianed start at "
                            + dsi.getExpectedStart() + " and end at " + dsi.getExpectedEnd());

                    fail("Action " + action + " has not been updated properly.\n" + "Expected start at " + times[coreId][actionIdx][0]
                            + " and end at " + times[coreId][actionIdx][1] + "\n" + "Obtianed start at " + dsi.getExpectedStart()
                            + " and end at " + dsi.getExpectedEnd());
                }
            }
        }
    }

    public static void verifyGaps(LinkedList<Gap> gaps, Gap[] expectedGaps) {
        for (Gap eg : expectedGaps) {
            boolean found = false;
            /*
             * Iterator<OptimizationElement<Gap>> gapsIt = gaps.iterator(); while (gapsIt.hasNext()) {
             * OptimizationElement<Gap> oe = gapsIt.next(); Gap g = oe.getElement();
             */
            Iterator<Gap> gapsIt = gaps.iterator();
            while (gapsIt.hasNext()) {
                Gap g = gapsIt.next();
                if (g.getInitialTime() == eg.getInitialTime() && g.getEndTime() == eg.getEndTime() && g.getOrigin() == eg.getOrigin()
                        && ((FakeResourceDescription) g.getResources()).checkEquals((FakeResourceDescription) eg.getResources())) {
                    gapsIt.remove();
                    found = true;
                    break;
                }
            }
            if (!found) {
                System.out.println("GAP " + eg + "  not found");
                fail("GAP " + eg + "  not found");
            }
        }
        if (gaps.size() > 0) {
            System.out.println("Obtianed unexpected gaps " + gaps);
            fail("Obtianed unexpected gaps " + gaps);
        }
    }

    public static void verifyInitialPlan(FakeAllocatableAction action, long executionTime, AllocatableAction... predecessors) {

        long start = 0;
        for (AllocatableAction predecessor : predecessors) {
            FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) predecessor.getSchedulingInfo();
            start = Math.max(start, dsi.getExpectedEnd());
        }
        long end = start + executionTime;

        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        if (dsi.getExpectedStart() != start) {
            System.out.println(action + " expected start time " + start + " obtained " + dsi.getExpectedStart());
            fail(action + " expected start time " + start + " obtained " + dsi.getExpectedStart());
        }

        if (dsi.getExpectedEnd() != end) {
            System.out.println(action + " expected end time " + end + " obtained " + dsi.getExpectedEnd());
            fail(action + " expected end time " + start + " obtained " + dsi.getExpectedEnd());
        }
        if (predecessors.length != dsi.getPredecessors().size()) {
            System.out.println(
                    action + " expected number of predecessors " + predecessors.length + " obtained " + dsi.getPredecessors().size() + "\n"
                            + "\tExpected :" + Arrays.asList(predecessors) + "\n" + "\tObtained :" + dsi.getPredecessors() + "\n");
            fail(action + " expected number of predecessors " + predecessors.length + " obtained " + dsi.getPredecessors().size());
        }
        for (AllocatableAction predecessor : predecessors) {
            if (!dsi.getPredecessors().contains(predecessor)) {
                System.out.println(predecessor + "expected to be a predecessor of " + action + " and it's not.\n" + "\tExpected :"
                        + Arrays.asList(predecessors) + "\n" + "\tObtained :" + dsi.getPredecessors() + "\n");
                fail(predecessor + "expected to be a predecessor of " + action + " and it's not");
            }
        }
    }

}
