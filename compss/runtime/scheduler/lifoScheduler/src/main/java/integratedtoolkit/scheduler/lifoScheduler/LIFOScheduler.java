package integratedtoolkit.scheduler.lifoScheduler;

import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.LIFOScore;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in LIFO mode
 * 
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class LIFOScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyScheduler<P, T, I> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);


    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public LIFOScheduler() {
        super();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public ResourceScheduler<P, T, I> generateSchedulerForResource(Worker<T, I> w) {
        //LOGGER.info("[LIFOScheduler] Generate scheduler for resource " + w.getName());
        return new LIFOResourceScheduler<P, T, I>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T, I> action) {
        //LOGGER.info("[LIFOScheduler] Generate Action Score for " + action);
        return new LIFOScore(action.getPriority(), 0, 0, (double) action.getId());
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public void handleDependencyFreeActions(LinkedList<AllocatableAction<P, T, I>> executionCandidates,
            LinkedList<AllocatableAction<P, T, I>> unassignedCandidates, LinkedList<AllocatableAction<P, T, I>> blockedCandidates) {

        // Schedules all possible free actions (LIFO type)

        //LOGGER.info("[LIFOScheduler] Treating dependency free actions");

        PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> executableActions = new PriorityQueue<>();
        for (AllocatableAction<P, T, I> action : executionCandidates) {
            Score actionScore = generateActionScore(action);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(action, actionScore);
            //LOGGER.debug("[LIFOScheduler] Releasing " + action);
            this.dependingActions.removeAction(action);
            executableActions.add(obj);
        }
        
        LinkedList<AllocatableAction<P, T, I>> currentUnassignedReadyActions = this.unassignedReadyActions.removeAllActions();
        for (AllocatableAction<P, T, I> action : currentUnassignedReadyActions) {
            Score actionScore = generateActionScore(action);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(action, actionScore);
     
            this.dependingActions.removeAction(action);
            executableActions.add(obj);
        }
        
        executionCandidates.clear();
        
        while (!executableActions.isEmpty()){
            ObjectValue<AllocatableAction<P, T, I>> actionObject = executableActions.poll();
            AllocatableAction<P, T, I> action = actionObject.getObject();
            Score actionScore = actionObject.getScore();
            //LOGGER.debug("[LIFOScheduler] Treating action " + action);
            try {
                action.schedule(actionScore);
                //LOGGER.debug("[LIFOScheduler] Action " + action + " scheduled in handleDependencyFreeActions");
                tryToLaunch(action);
                //LOGGER.debug("[LIFOScheduler] Action " + action + " successfully launched");
                //executionCandidates.add(action);
            } catch (UnassignedActionException ex) {
                //LOGGER.debug("[LIFOScheduler] Adding action " + action + " to unassigned list");
                this.unassignedReadyActions.addAction(action);
            } catch (BlockedActionException e) {
                //LOGGER.debug("[LIFOScheduler] Adding action " + action + " to the blocked list");
                blockedCandidates.add(action);
            }    
        }

    }

}
