/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.invokers.types;

import java.util.LinkedList;
import java.util.List;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores the task status information for bindings
 *
 */
public class ExternalTaskStatus {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private final Integer exitValue;
    private final List<DataType> updatedParameterTypes;
    private final List<String> updatedParameterValues;

    /**
     * Creates a new task status instance with exitValue
     *
     * @param exitValue
     */
    public ExternalTaskStatus(Integer exitValue) {
        this.exitValue = exitValue;
        this.updatedParameterTypes = new LinkedList<>();
        this.updatedParameterValues = new LinkedList<>();
    }

    public ExternalTaskStatus(String[] line) {
        this.updatedParameterTypes = new LinkedList<>();
        this.updatedParameterValues = new LinkedList<>();

        // Line of the form: "endTask" ID STATUS D paramType1 paramValue1 ... paramTypeD paramValueD
        this.exitValue = Integer.parseInt(line[2]);

        // Process parameters if message contains them
        if (line.length > 3) {
            int numParams = Integer.parseInt(line[3]);

            if (4 + 2 * numParams != line.length) {
                LOGGER.warn("WARN: Skipping endTask parameters because of malformation.");
                numParams = (line.length - 4) / 2;
            } 
            // Process parameters
            for (int i = 0; i < numParams; ++i) {
                int paramTypeOrdinalIndex = 0;
                try {
                    paramTypeOrdinalIndex = Integer.parseInt(line[4 + 2 * i]);
                } catch (NumberFormatException nfe) {
                    LOGGER.warn("WARN: Number format exception on " + line[4 + 2 * i] + ". Setting type 0", nfe);
                }
                DataType paramType = DataType.values()[paramTypeOrdinalIndex];

                String paramValue = line[5 + 2 * i];
                if (paramValue.equalsIgnoreCase("null")) {
                    paramValue = null;
                }
                addParameter(paramType, paramValue);
            }
        } else {
            LOGGER.warn("WARN: endTask message does not have task result parameters");
        }
    }

    /**
     * Returns the exitValue of the task (null if it has not ended yet)
     *
     * @return
     */
    public Integer getExitValue() {
        return this.exitValue;
    }

    /**
     * Returns the number of parameters of the task
     *
     * @return
     */
    public int getNumParameters() {
        return this.updatedParameterValues.size();
    }

    /**
     * Returns all the parameters' types
     *
     * @return
     */
    public List<DataType> getParameterTypes() {
        return this.updatedParameterTypes;
    }

    /**
     * Returns all the parameters' values
     *
     * @return
     */
    public List<String> getParameterValues() {
        return this.updatedParameterValues;
    }

    /**
     * Returns the i-th parameter type. Null if i is out of the parameters range
     *
     * @param i
     * @return
     */
    public DataType getParameterType(int i) {
        if (i >= 0 && i < this.updatedParameterTypes.size()) {
            return this.updatedParameterTypes.get(i);
        }
        return null;
    }

    /**
     * Returns the i-th parameter value. Null if i is out of the parameters range
     *
     * @param i
     * @return
     */
    public String getParameterValue(int i) {
        if (i >= 0 && i < this.updatedParameterValues.size()) {
            return this.updatedParameterValues.get(i);
        }
        return null;
    }

    /**
     * Adds a new parameter
     *
     * @param type
     * @param value
     */
    public void addParameter(DataType type, String value) {
        this.updatedParameterTypes.add(type);
        this.updatedParameterValues.add(value);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExternalTaskStatus [ ");

        sb.append("ExitValue = ").append(this.exitValue).append(", ");
        sb.append("NumParameters = ").append(getNumParameters()).append(", ");
        sb.append("ParameterTypes = [");
        for (DataType type : this.updatedParameterTypes) {
            sb.append(type.ordinal()).append(" ");
        }
        sb.append("], ");
        sb.append("ParameterValues = [");
        for (String value : this.updatedParameterValues) {
            sb.append(value).append(" ");
        }
        sb.append("]");

        sb.append(" ]");

        return sb.toString();
    }

}
