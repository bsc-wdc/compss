/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.accesses;

import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.components.monitor.impl.GraphHandler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Class handling all the accesses related to a data value.
 */
public abstract class DataAccessesInfo {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected static final boolean IS_DRAW_GRAPH = GraphGenerator.isEnabled();

    private final DataType dataType;


    /**
     * Constructs a new AccessInfo according to the type of data being accessed.
     *
     * @param type type of data being accessed
     * @return AccessInfo to handle accesses to the data
     */
    public static DataAccessesInfo createAccessInfo(DataType type) {
        DataAccessesInfo ai;
        switch (type) {
            case STREAM_T:
            case EXTERNAL_STREAM_T:
                ai = new StreamDataAccessesInfo(type);
                break;
            default:
                ai = new StandardDataAccessesInfo(type);
                break;
        }
        return ai;
    }

    protected DataAccessesInfo(DataType type) {
        this.dataType = type;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    /**
     * Registers a data producer as completed.
     * 
     * @param task Data Producer
     * @param gh class handling the requests to add edges to the monitoring graph
     */
    public abstract void completedProducer(AbstractTask task, GraphHandler gh);

    public abstract AbstractTask getConstrainingProducer();

    /**
     * Registers a task reading the data value.
     *
     * @param t task reading the value
     * @param dp parameter corresponding to the data value
     * @param isConcurrent {@literal true} if the reading was due to a concuerrent access; {@literal false} otherwise.
     * @param gh class handling the requests to add edges to the monitoring graph
     * @return {@literal true}, if an edge has been printed; {@literal false}, otherwise.
     */
    public abstract boolean readValue(Task t, DependencyParameter dp, boolean isConcurrent, GraphHandler gh);

    /**
     * Registers a task writting on the data value.
     *
     * @param t task writting the value
     * @param dp parameter corresponding to the data value
     * @param isConcurrent {@literal true} if the writting was due to a concuerrent access; {@literal false} otherwise.
     * @param gh class handling the requests to add edges to the monitoring graph
     */
    public abstract void writeValue(Task t, DependencyParameter dp, boolean isConcurrent, GraphHandler gh);

    /**
     * Registers an access from the application main code to the value.
     *
     * @param rdar Request to access the data value
     * @param gh class handling the requests to add edges to the monitoring graph
     * @param dataId data id
     * @param dataVersion version id
     */
    public abstract void mainAccess(RegisterDataAccessRequest rdar, GraphHandler gh, int dataId, int dataVersion);

    /**
     * Checks whether t is responsible for the generation of the last version of the value.
     * 
     * @param t task to check if it was the last producer of the value
     * @return {@literal true} if t was the last task generating the value; {@literal false} otherwise.
     */
    public abstract boolean isFinalProducer(Task t);

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("WI [ ");
        sb.append("dataType = ").append(this.dataType).append(", ");
        sb.append(this.toStringDetails());
        sb.append("]");

        return sb.toString();
    }

    protected abstract String toStringDetails();

    /**
     * Returns the last Tasks producing the value.
     * 
     * @return last tasks generating the value.
     */
    public abstract List<AbstractTask> getDataWriters();

}
