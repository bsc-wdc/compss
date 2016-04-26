package integratedtoolkit.scheduler.defaultscheduler.utils;

import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.types.Gap;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.fake.FakeAllocatableAction;
import integratedtoolkit.types.fake.FakeResourceDescription;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import static org.junit.Assert.fail;

public class Verifiers {

    public static void verifyScore(DefaultScore ds, long action, long data, long res, long impl, long start) {
        if (action != ds.getActionScore()) {
            System.out.println("Scores do not match. Expected action score " + action + " and " + ds.getActionScore() + " obtianed.");
            fail("Scores do not match. Expected action score " + action + " and " + ds.getActionScore() + " obtianed.");
        }
        if (data != ds.getExpectedDataAvailable()) {
            System.out.println("Scores do not match. Expected data available at " + data + " and " + ds.getExpectedDataAvailable() + " obtianed.");
            fail("Scores do not match. Expected data available at " + data + " and " + ds.getExpectedDataAvailable() + " obtianed.");
        }
        if (res != ds.getResourceScore()) {
            System.out.println("Scores do not match. Expected resource score " + res + " and " + ds.getResourceScore() + " obtianed.");
            fail("Scores do not match. Expected resource score " + res + " and " + ds.getResourceScore() + " obtianed.");
        }
        if (impl != ds.getImplementationScore()) {
            System.out.println("Scores do not match. Expected implementation score " + impl + " and " + ds.getImplementationScore() + " obtianed.");
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

    public static void verifyUpdate(FakeAllocatableAction[] actions, long[][] times) {
        for (int actionIdx = 0; actionIdx < actions.length; actionIdx++) {
            DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) actions[actionIdx].getSchedulingInfo();
            if (dsi.getExpectedStart() != times[actionIdx][0] || dsi.getExpectedEnd() != times[actionIdx][1]) {

                System.out.println("Action " + actions[actionIdx] + " has not been updated properly.\n"
                        + "Expected start at " + times[actionIdx][0] + " and end at " + times[actionIdx][1] + "\n"
                        + "Obtianed start at " + dsi.getExpectedStart() + " and end at " + dsi.getExpectedEnd());

                fail("Action " + actions[actionIdx] + " has not been updated properly.\n"
                        + "Expected start at " + times[actionIdx][0] + " and end at " + times[actionIdx][1] + "\n"
                        + "Obtianed start at " + dsi.getExpectedStart() + " and end at " + dsi.getExpectedEnd());
            }
        }
    }

    public static void verifyGaps(LinkedList<Gap> gaps, Gap[] expectedGaps) {
        for (Gap eg : expectedGaps) {
            boolean found = false;
            /*Iterator<OptimizationElement<Gap>> gapsIt = gaps.iterator();
             while (gapsIt.hasNext()) {
             OptimizationElement<Gap> oe = gapsIt.next();
             Gap g = oe.getElement();*/
            Iterator<Gap> gapsIt = gaps.iterator();
            while (gapsIt.hasNext()) {
                Gap g = gapsIt.next();
                if (g.getInitialTime() == eg.getInitialTime()
                        && g.getEndTime() == eg.getEndTime()
                        && g.getOrigin() == eg.getOrigin()
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
            DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) predecessor.getSchedulingInfo();
            start = Math.max(start, dsi.getExpectedEnd());
        }
        long end = start + executionTime;

        DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) action.getSchedulingInfo();
        if (dsi.getExpectedStart() != start) {
            System.out.println(action + " expected start time " + start + " obtained " + dsi.getExpectedStart());
            fail(action + " expected start time " + start + " obtained " + dsi.getExpectedStart());
        }

        if (dsi.getExpectedEnd() != end) {
            System.out.println(action + " expected end time " + end + " obtained " + dsi.getExpectedEnd());
            fail(action + " expected end time " + start + " obtained " + dsi.getExpectedEnd());
        }
        if (predecessors.length != dsi.getPredecessors().size()) {
            System.out.println(action + " expected number of predecessors " + predecessors.length + " obtained " + dsi.getPredecessors().size() + "\n"
                    + "\tExpected :" + Arrays.asList(predecessors) + "\n"
                    + "\tObtained :" + dsi.getPredecessors() + "\n"
            );
            fail(action + " expected number of predecessors " + predecessors.length + " obtained " + dsi.getPredecessors().size());
        }
        for (AllocatableAction predecessor : predecessors) {
            if (!dsi.getPredecessors().contains(predecessor)) {
                System.out.println(predecessor + "expected to be a predecessor of " + action + " and it's not.\n"
                        + "\tExpected :" + Arrays.asList(predecessors) + "\n"
                        + "\tObtained :" + dsi.getPredecessors() + "\n");
                fail(predecessor + "expected to be a predecessor of " + action + " and it's not");
            }
        }
    }
}
