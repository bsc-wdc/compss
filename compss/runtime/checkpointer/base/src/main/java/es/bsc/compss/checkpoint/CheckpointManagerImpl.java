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
package es.bsc.compss.checkpoint;

import es.bsc.compss.checkpoint.types.CheckpointGroupImpl;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.info.DataVersion;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.Tracer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class CheckpointManagerImpl extends CheckpointRecord implements CheckpointManager, CheckpointUtils {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.CP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // GLOBAL STATIC VALUES
    private static final String PERIOD_TIME = "period.time";
    private static final String FINISHED_TASKS = "finished.tasks";
    private static final String INSTANTIATED_GROUP = "instantiated.group";
    private static final String AVOID_CHECKPOINT = "avoid.checkpoint";

    private static final int DEF_GROUP_SIZE = 3;

    // CONFIGURATION VARIABLES
    private int finishedTasksCounter = 0; // Number of tasks finished comping from a Finished tasks group
    protected int finishedTasksSpan = 0; // Size of the finished tasks group
    private long time = 0; // Period of checkpoint
    protected int defGroup = -1; // Default group for snapshot for time and endTask policies
    protected int countingGroup = 1; // Number of the group
    protected int groupSize = 0; // Size of a task group
    private Timer timer = null; // Timer managing time checkpointing

    // List of tasks that do not have to be checkpointed
    private final HashSet<String> avoidCheckpointTasks = new HashSet<>();
    private final HashMap<String, CheckpointGroupImpl> openGroups; // Checkpointed groups


    /**
     * CheckpointImpl constructor.
     *
     * @param config Hashmap with the checkpoint configuration.
     * @param time Period checkpoint time value.
     * @param finishedTasksSpan Size of the group of finished tasks.
     * @param cpUser Instance of AccessProcessor.
     */
    public CheckpointManagerImpl(HashMap<String, String> config, long time, int finishedTasksSpan, User cpUser) {
        super(cpUser);
        this.openGroups = new HashMap<>();
        setConfig(config, time, finishedTasksSpan);
    }

    @Override
    public final void newTask(Task t) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_NEW_TASK);
        }
        if (isTaskCheckpointed(t)) {
            recoverTask(t);
            if (DEBUG) {
                LOGGER.debug("Task " + t.getId() + " was recovered from a previous checkpoint");
            }
        } else {
            String signature = t.getTaskDescription().getCoreElement().getSignature();
            if (this.avoidCheckpointTasks.contains(signature)) {
                if (DEBUG) {
                    LOGGER.debug("Task " + t.getId() + " checkpointing ignored due to signature " + signature);
                }
            } else {
                assignTaskToGroup(t);
                registerTask(t);
                if (DEBUG) {
                    LOGGER.debug("Task " + t.getId() + " will be checkpointed.");
                }
            }
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_NEW_TASK);
        }
    }

    /**
     * Assigns task to group.
     *
     * @param t Task to be assigned to a group.
     */
    protected abstract void assignTaskToGroup(Task t);

    @Override
    public final void endTask(Task t) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_END_TASK);
        }
        String signature = t.getTaskDescription().getCoreElement().getSignature();
        if (!this.avoidCheckpointTasks.contains(signature)) {
            completedTask(t);

            CheckpointGroupImpl group = (CheckpointGroupImpl) t.getCheckpointGroup();
            if (group != null) {
                group.addFinishedTask(t);
                if (group.getState() == CheckpointGroupImpl.GroupState.CLOSED) {
                    List<DataVersion> allowedValues = group.getOutputData();
                    requestTaskCheckpoint(t, allowedValues);
                }
            }

            if (finishedTasksSpan != 0 || time != 0) {
                // If we are using finished tasks or periodic time policies, keep track of the finished tasks
                finishedTasksCounter += 1;
                if (finishedTasksCounter == finishedTasksSpan) {
                    requestSaveLastDataVersions();
                    finishedTasksCounter -= finishedTasksSpan;
                }
            }
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_NEW_TASK);
        }
    }

    @Override
    public final void snapshot() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_SNAPSHOT);
        }
        for (CheckpointGroupImpl group : openGroups.values()) {
            closeGroup(group);
        }
        openGroups.clear();

        this.defGroup -= 1;
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_SNAPSHOT);
        }
    }

    @Override
    public final CheckpointGroupImpl addTaskToGroup(Task t, String groupName) {
        CheckpointGroupImpl group = openGroups.get(groupName);
        if (group == null) {
            LOGGER.debug("Creating checkpoint group " + groupName);
            group = new CheckpointGroupImpl(groupName);
            openGroups.put(groupName, group);
        }
        t.setCheckpointGroup(group);
        for (Parameter param : t.getParameters()) {
            registerTaskParameterInGroup(param, group);
        }
        if (DEBUG) {
            LOGGER.debug("Task " + t.getId() + " assigned to checkpoint group " + groupName);
        }
        group.addTask(t);
        return group;
    }

    /**
     * Processes the parameters when are added to a group.
     *
     * @param param Parameter to be processed.
     * @param group Group that will have the parameter.
     */
    private void registerTaskParameterInGroup(Parameter param, CheckpointGroupImpl group) {
        if (param.isCollective()) {
            CollectiveParameter cp = (CollectiveParameter) param;
            for (Parameter sp : cp.getElements()) {
                registerTaskParameterInGroup(sp, group);
            }
        } else {
            if (param.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) param;
                registerTaskSimpleParameterInGroup(dp, group);
            } else {
                LOGGER.warn("Checkpoint does nor support parameter type " + param.getType());
            }
        }
    }

    private void registerTaskSimpleParameterInGroup(DependencyParameter dp, CheckpointGroupImpl group) {
        EngineDataAccessId paramId = dp.getDataAccessId();
        DataVersion outDV = null;
        if (paramId.isWrite()) {
            outDV = ((WritingDataAccessId) paramId).getWrittenDataVersion();
        }
        if (outDV != null) {
            group.producesData(outDV);
        }
    }

    @Override
    public final void closeGroup(String groupName) {
        CheckpointGroupImpl group = openGroups.remove(groupName);
        if (group != null) {
            closeGroup(group);
        }
    }

    private void closeGroup(CheckpointGroupImpl group) {
        String groupName = group.getName();
        LOGGER.debug("Closing checkpoint group " + groupName);
        if (group.getState() == CheckpointGroupImpl.GroupState.CLOSED) {
            if (DEBUG) {
                LOGGER.warn("Checkpoint group " + groupName + " already closed");
            }
        } else {
            group.close();
            // When we close the group we ask to save the data of the tasks
            for (Task t : group.getFinishedTasks()) {
                requestTaskCheckpoint(t, group.getOutputData());
            }
        }
    }

    @Override
    public final void shutdown() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_SHUTDOWN);
        }
        for (CheckpointGroupImpl group : this.openGroups.values()) {
            closeGroup(group);
        }
        openGroups.clear();

        if (this.timer != null) {
            this.timer.cancel();
        }

        performAllCopies();
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_SHUTDOWN);
        }
    }

    // SET CONFIG
    /**
     * Sets the checkpointer configuration.
     *
     * @param config Hashmap with the checkpointer configuration.
     * @param time periodic time to checkpoint.
     * @param finishedTasksSpan is the span for which the checkpointer to checkpoint finished tasks.
     */
    private void setConfig(HashMap<String, String> config, long time, int finishedTasksSpan) {
        if (config.size() > 0) {
            this.time = config.get(PERIOD_TIME) != null ? Long.parseLong(config.get(PERIOD_TIME)) : time;
            this.finishedTasksSpan =
                config.get(FINISHED_TASKS) != null ? Integer.parseInt(config.get(FINISHED_TASKS)) : finishedTasksSpan;
            this.groupSize = config.get(INSTANTIATED_GROUP) != null ? Integer.parseInt(config.get(INSTANTIATED_GROUP))
                : DEF_GROUP_SIZE;
            if (config.get(AVOID_CHECKPOINT) != null) {
                String[] avoidCheckpoint =
                    config.get(AVOID_CHECKPOINT).substring(1, config.get(AVOID_CHECKPOINT).length() - 1).split(",");
                Collections.addAll(this.avoidCheckpointTasks, avoidCheckpoint);
            }
        } else {
            this.time = time;
            this.finishedTasksSpan = finishedTasksSpan;
            this.groupSize = DEF_GROUP_SIZE;
        }
        if (this.time > 0) {
            setTimerPolicy();
        }
        if (DEBUG) {
            LOGGER.debug("Checkpoint configuration: \n" + "\tTime Interval: " + this.time + "\n"
                + "\tFinished Tasks Span: " + this.finishedTasksSpan + "\n" + "\tGroup Size: " + this.groupSize + "\n");
        }
    }

    /**
     * Sets the timer configuration.
     */
    private void setTimerPolicy() {
        this.timer = new Timer();
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                requestSaveLastDataVersions();
            }
        }, time, time);
    }

}
