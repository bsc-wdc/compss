/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.resources.ResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class LocalOptimizationState {

    private final long updateId;

    private final LinkedList<Gap> gaps;

    private AllocatableAction action;
    private ResourceDescription missingResources;
    private long topStartTime;


    /**
     * Creates a new LocalOptimizationState instance.
     * 
     * @param updateId Optimization id.
     * @param rd Associated resouce description.
     */
    public LocalOptimizationState(long updateId, ResourceDescription rd) {
        this.action = null;

        this.updateId = updateId;

        this.gaps = new LinkedList<>();
        Gap g = new Gap(0, Long.MAX_VALUE, null, rd.copy(), 0);
        this.gaps.add(g);
    }

    /**
     * Returns the optimization Id.
     * 
     * @return The optimization Id.
     */
    public long getId() {
        return this.updateId;
    }

    /**
     * Reserves the given resources from the given start time.
     * 
     * @param resources Resources to reserve.
     * @param startTime Starting time.
     * @return List of previous gaps.
     */
    public List<Gap> reserveResources(ResourceDescription resources, long startTime) {
        List<Gap> previousGaps = new LinkedList<>();
        // Remove requirements from resource description
        ResourceDescription requirements = resources.copy();
        Iterator<Gap> gapIt = this.gaps.iterator();
        while (gapIt.hasNext() && !requirements.isDynamicUseless()) {
            Gap g = gapIt.next();
            if (checkGapForReserve(g, requirements, startTime, previousGaps)) {
                gapIt.remove();
            }
        }

        return previousGaps;
    }

    private boolean checkGapForReserve(Gap g, ResourceDescription requirements, long reserveStart,
            List<Gap> previousGaps) {

        boolean remove = false;
        AllocatableAction gapAction = g.getOrigin();
        ResourceDescription rd = g.getResources();
        ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(rd, requirements);
        Gap tmpGap = new Gap(g.getInitialTime(), reserveStart, g.getOrigin(), reduction, 0);
        previousGaps.add(tmpGap);

        if (gapAction != null) {
            FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
            // Remove resources from the first gap
            gapDSI.addGap();
        }

        // If the gap has been fully used
        if (rd.isDynamicUseless()) {
            // Remove the gap
            remove = true;
            if (gapAction != null) {
                FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
                gapDSI.removeGap();
            }
        }

        return remove;
    }

    /**
     * Releases the resources allocated by the given action at the given start time.
     * 
     * @param expectedStart Expected start time.
     * @param action Action.
     */
    public void releaseResources(long expectedStart, AllocatableAction action) {
        Gap gap = new Gap(expectedStart, Long.MAX_VALUE, action, action.getAssignedImplementation().getRequirements(),
                0);
        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        dsi.addGap();
        this.gaps.add(gap);
        if (this.missingResources != null) {
            ResourceDescription empty = gap.getResources().copy();
            this.topStartTime = gap.getInitialTime();
            ResourceDescription.reduceCommonDynamics(empty, this.missingResources);
        }
    }

    /**
     * Replaces the action by the given one.
     * 
     * @param action New action.
     */
    public void replaceAction(AllocatableAction action) {
        this.action = action;
        if (this.action != null) {
            missingResources = this.action.getAssignedImplementation().getRequirements().copy();
            // Check if the new peek can run in the already freed resources.
            for (Gap gap : this.gaps) {
                ResourceDescription empty = gap.getResources().copy();
                this.topStartTime = gap.getInitialTime();
                ResourceDescription.reduceCommonDynamics(empty, this.missingResources);
                if (this.missingResources.isDynamicUseless()) {
                    break;
                }
            }
        } else {
            this.missingResources = null;
            this.topStartTime = 0L;
        }
    }

    /**
     * Add temporary gap.
     * 
     * @param g Temporary gap.
     */
    public void addTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
        gapDSI.addGap();
    }

    /**
     * Replace temporary gap.
     * 
     * @param gap New gap.
     * @param previousGap Previous gap.
     */
    public void replaceTmpGap(Gap gap, Gap previousGap) {
        // TODO: Replace tmp gap
    }

    /**
     * Removes the given gap.
     * 
     * @param g Gap to remove.
     */
    public void removeTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        if (gapAction != null) {
            FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
            gapDSI.removeGap();
            if (!gapDSI.hasGaps()) {
                gapDSI.unlock();
            }
        }
    }

    /**
     * Returns the associated action.
     * 
     * @return The associated action.
     */
    public AllocatableAction getAction() {
        return this.action;
    }

    /**
     * Returns the maximum action start time.
     * 
     * @return The maximum action start time.
     */
    public long getActionStartTime() {
        return Math.max(this.topStartTime,
                ((FullGraphSchedulingInformation) this.action.getSchedulingInfo()).getExpectedStart());
    }

    /**
     * Returns whether the action can run or not.
     * 
     * @return {@literal true} if the action can run, {@literal false} otherwise.
     */
    public boolean canActionRun() {
        if (this.missingResources != null) {
            return this.missingResources.isDynamicUseless();
        } else {
            return false;
        }
    }

    /**
     * Returns whether there are gaps or not.
     * 
     * @return {@literal true} if there are gaps, {@literal false} otherwise.
     */
    public boolean areGaps() {
        return !this.gaps.isEmpty();
    }

    /**
     * Returns the first available gap.
     * 
     * @return The first available gap.
     */
    public Gap peekFirstGap() {
        return this.gaps.peekFirst();
    }

    /**
     * Removes the first available gap.
     */
    public void pollGap() {
        this.gaps.removeFirst();
    }

    /**
     * Returns all the registered gaps.
     * 
     * @return A list of the registered gaps.
     */
    public LinkedList<Gap> getGaps() {
        return this.gaps;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Optimization State at " + this.updateId + "\n");
        sb.append("\tGaps:\n");
        for (Gap gap : this.gaps) {
            sb.append("\t\t").append(gap).append("\n");
        }
        sb.append("\tTopAction:").append(this.action).append("\n");
        sb.append("\tMissing To Run:").append(this.missingResources).append("\n");
        sb.append("\tExpected Start:").append(this.topStartTime).append("\n");
        return sb.toString();
    }

}
