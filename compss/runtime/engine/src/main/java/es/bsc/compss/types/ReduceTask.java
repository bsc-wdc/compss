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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.colors.ColorConfiguration;
import es.bsc.compss.types.colors.ColorNode;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.FileParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ReduceTask extends Task {

    private static final ParameterMonitor IGNORE_PARAM = new ParameterMonitor() {

        @Override
        public void onCreation(DataType type, String dataName) {
            // Ignore Notification
        }
    };
    // Tasks that access the data
    private final List<Task> tasks;

    private int chunkSize;
    private double totalOperations;
    // Available Input parameters for partial tasks
    private final List<Parameter> partialsIn;
    // Input parameters already used by generated partial tasks.
    private final List<Parameter> usedPartialsIn;
    // Available Output parameters for partial task
    private final List<Parameter> partialsOut;
    // Input parameters already used by generated partial tasks
    private final List<Parameter> usedPartialsOut;
    // Available Collection parameters for partial task
    private final List<CollectiveParameter> intermediateCollections;
    private int reduceCollectionIndex = -1;
    private CollectiveParameter finalCol;

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);


    /**
     * Creates a new REDUCE task with the given parameters.
     *
     * @param app Application to which the task belongs.
     * @param lang Application language.
     * @param signature Task signature.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of nodes used by the task.
     * @param reduceChunkSize Size of the chunks to execute the reduce.
     * @param isReduction Whether the task must be replicated or not.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed round-robin or not.
     * @param numReturns Number of returns of the task.
     * @param hasTarget Whether the task has a target object or not.
     * @param parameters Task parameter values.
     * @param monitor Task monitor.
     * @param onFailure On failure mechanisms.
     * @param timeOut Time for a task time out.
     */
    public ReduceTask(Application app, Lang lang, String signature, boolean isPrioritary, int numNodes,
        boolean isReduction, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        int numReturns, List<Parameter> parameters, TaskMonitor monitor, OnFailure onFailure, long timeOut) {

        super(app, lang, signature, isPrioritary, numNodes, isReduction, isReplicated, isDistributed, hasTarget,
            numReturns, parameters, monitor, onFailure, timeOut);
        this.tasks = new LinkedList<>();
        this.chunkSize = reduceChunkSize;
        this.totalOperations = 0;
        this.partialsIn = new ArrayList<>();
        this.usedPartialsIn = new ArrayList<>();
        this.partialsOut = new ArrayList<>();
        this.usedPartialsOut = new ArrayList<>();
        this.intermediateCollections = new ArrayList<>();
        LOGGER.debug("[REDUCE-TASK] The REDUCE task has been created with chunk size " + this.chunkSize);

        try {
            createPartialParameters(parameters);
        } catch (IOException e) {
            LOGGER.debug("Exception detected when creating location for partials");
        }
    }

    /**
     * Creates the parameters to be fulfilled by the reduce tasks.
     *
     * @param parameters Task parameter values.
     * @throws IOException Error while creating the data location.
     */
    private void createPartialParameters(List<Parameter> parameters) throws IOException {
        if (parameters.size() >= 2) {
            // 0 --> collection || 1 --> result

            this.reduceCollectionIndex = searchFirstCollection(parameters);
            Parameter finalParameter = parameters.get(parameters.size() - 1);
            if (finalParameter.getDirection() == Direction.OUT && this.reduceCollectionIndex >= 0) {
                CollectiveParameter p = (CollectiveParameter) parameters.get(this.reduceCollectionIndex);
                List<Parameter> colList = p.getElements();
                if (colList.size() < 2) {
                    ErrorManager.warn("Reduce collection of Task " + getId() + " has not two parameters to reduce");
                }
                // Calculate maximum number of operations
                double completeOperations = 0;
                this.totalOperations = ResourceManager.getTotalNumberOfWorkers() + 1;
                double intermediateResults = 0;
                double accum = colList.size();
                while (accum > chunkSize) {
                    completeOperations = Math.floor(accum / this.chunkSize);
                    intermediateResults = accum % chunkSize;
                    accum = completeOperations + intermediateResults;
                    this.totalOperations = this.totalOperations + accum;
                }
                LOGGER.debug("[REDUCE-TASK] Creating intermediate data (" + this.totalOperations + ") for reduce Task "
                    + this.getId());

                Application app = this.getApplication();
                for (int i = 0; i < (int) totalOperations; i++) {
                    String partialId = "reduce" + i + "PartialResultTask" + this.getId();
                    String canonicalPath = new File(partialId).getCanonicalPath();
                    SimpleURI uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + canonicalPath);
                    DataLocation dl = DataLocation.createLocation(Comm.getAppHost(), uri);

                    partialsOut.add(FileParameter.newFP(app, Direction.OUT, finalParameter.getStream(),
                        finalParameter.getPrefix(), finalParameter.getName(), finalParameter.getType().toString(),
                        finalParameter.getWeight(), finalParameter.isKeepRename(), dl, partialId, IGNORE_PARAM));
                    partialsIn.add(FileParameter.newFP(app, Direction.IN, finalParameter.getStream(),
                        finalParameter.getPrefix(), finalParameter.getName(), finalParameter.getType().toString(),
                        finalParameter.getWeight(), finalParameter.isKeepRename(), dl, partialId, IGNORE_PARAM));

                    CollectiveParameter cp = CollectiveParameter.newCP(app, DataType.COLLECTION_T,
                        partialId + "Collection", p.getDirection(), p.getStream(), p.getPrefix(), p.getName(),
                        p.getContentType(), p.getWeight(), p.isKeepRename(), IGNORE_PARAM, new ArrayList<>());
                    intermediateCollections.add(cp);
                }
                String finalId = "finalReduceTask" + this.getId();
                finalCol = CollectiveParameter.newCP(app, DataType.COLLECTION_T, finalId, Direction.IN, p.getStream(),
                    p.getPrefix(), p.getName(), p.getContentType(), p.getWeight(), p.isKeepRename(), IGNORE_PARAM,
                    new ArrayList<>());
            } else {
                ErrorManager
                    .fatal("First parameter for a reduce task must be a collection and last parameter must be OUT "
                        + "or must have return");
            }
        } else {
            ErrorManager.fatal("Incorrect number of parameters for a reduce task. It should be higher than 2");
        }
    }

    private int searchFirstCollection(List<Parameter> parameters) {
        for (int i = 0; i < parameters.size() - 1; i++) {
            if (parameters.get(i).getType() == DataType.COLLECTION_T) {
                return i;
            }
        }
        return -1; // Collection Not found
    }

    public int getReduceCollectionIndex() {
        return this.reduceCollectionIndex;
    }

    /**
     * Returns the list of IN parameters.
     */
    public List<Parameter> getIntermediateInParameters() {
        return partialsIn;
    }

    /**
     * Returns the list of used IN parameters.
     */
    public List<Parameter> getIntermediateUsedInParameters() {
        return usedPartialsIn;
    }

    /**
     * Sets the parameter to the list of used IN parameters.
     */
    public void setPartialInUsed(Parameter partial) {
        usedPartialsIn.add(partial);
        partialsIn.remove(partial);
    }

    /**
     * Returns the list of OUT parameters.
     */
    public List<Parameter> getIntermediateOutParameters() {
        return partialsOut;
    }

    /**
     * Returns the list of used OUT parameters.
     */
    public List<Parameter> getIntermediateUsedOutParameters() {
        return usedPartialsOut;
    }

    /**
     * Sets the parameter to the list of used OUT parameters.
     */
    public void setPartialOutUsed(Parameter partial) {
        usedPartialsOut.add(partial);
        partialsOut.remove(partial);
    }

    /**
     * Returns the list of the created intermediate collections.
     */
    public List<CollectiveParameter> getIntermediateCollections() {
        return intermediateCollections;
    }

    /**
     * Returns the list of the created intermediate collections.
     */
    public CollectiveParameter getFinalCollection() {
        return finalCol;
    }

    /**
     * Returns list of tasks to execute.
     * 
     * @return
     */
    public List<Task> getTasks() {
        return tasks;
    }

    /**
     * Returns the reduce chunk size.
     * 
     * @return
     */
    public int getChunkSize() {
        return this.chunkSize;
    }

    /**
     * Clears the partial parameters.
     */
    public void clearPartials() {
        this.partialsIn.clear();
        this.partialsOut.clear();
        this.intermediateCollections.clear();
    }

    private List<Parameter> getUnusedIntermediateParameters() {
        this.partialsIn.addAll(this.partialsOut);
        return partialsIn;
    }

    /**
     * Returns the total number of operations.
     * 
     * @return The number of operations.
     */
    public int getTotalOperations() {
        return (int) this.totalOperations;
    }

    @Override
    public String getColor() {
        int monitorTaskId = this.getTaskDescription().getCoreElement().getCoreId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.getColors()[monitorTaskId % (ColorConfiguration.NUM_COLORS + 1)];
        return color.getFillColor();
    }

    private List<Parameter> getParameterDataToRemove() {
        List<Parameter> dataToRemove = new LinkedList<>();
        dataToRemove.addAll(getIntermediateInParameters());
        dataToRemove.addAll(getIntermediateOutParameters());
        dataToRemove.addAll(getIntermediateCollections());
        return dataToRemove;
    }

    private List<Parameter> getIntermediateParameters() {
        List<Parameter> interParams = new LinkedList<>();
        // The order matters
        interParams.addAll(getIntermediateOutParameters());
        interParams.addAll(getIntermediateInParameters());
        interParams.addAll(getIntermediateCollections());
        interParams.add(finalCol);
        return interParams;
    }

    @Override
    protected boolean registerTask() {
        boolean hasEdge = super.registerTask();
        // Register Intermediate parameters
        for (Parameter p : this.getIntermediateParameters()) {
            p.register(this, false);
        }

        // Marking intermediate parameters for deletion
        for (Parameter p : this.getParameterDataToRemove()) {
            try {
                p.removeData();
            } catch (ValueUnawareRuntimeException e) {
                // If not existing, the parameter was already removed. No need to do anything
            }
        }
        return hasEdge;
    }

    protected void commitParams() {
        super.commitParams();
        for (Parameter param : this.getUnusedIntermediateParameters()) {
            param.commit(this);
        }
    }

    protected void cancelParams() {
        super.cancelParams();
        for (Parameter param : this.getUnusedIntermediateParameters()) {
            param.cancel(this);
        }
    }
}
