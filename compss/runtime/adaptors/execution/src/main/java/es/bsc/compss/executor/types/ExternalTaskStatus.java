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
package es.bsc.compss.executor.types;

import es.bsc.compss.executor.types.ParameterResult.CollectiveResult;
import es.bsc.compss.executor.types.ParameterResult.SingleResult;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores the task status information for bindings.
 */
public class ExternalTaskStatus {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    private static final String CANNOT_PARSE_ERROR = "Could not parse Collection results";

    private final Integer exitValue;
    private final List<ParameterResult> results;


    /**
     * Creates a new task status instance with exitValue.
     *
     * @param exitValue Exit Value
     */
    public ExternalTaskStatus(Integer exitValue) {
        this.exitValue = exitValue;
        this.results = new LinkedList<>();
    }

    /**
     * Creates a new task status instance with exitValue.
     *
     * @param line task status content as string array
     */
    public ExternalTaskStatus(String[] line) {
        this.results = new LinkedList<>();

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
                this.results.add(createResult(paramType, paramValue));
            }
        } else {
            LOGGER.warn("WARN: endTask message does not have task result parameters");
        }
    }

    /**
     * Adds a new parameter.
     *
     * @param type Parameter Type
     * @param value Parameter Value
     * @return object describing the results
     */
    private ParameterResult createResult(DataType type, String value) {
        if (type == DataType.COLLECTION_T) {
            return new CollectiveResult(type, createCollectionResultValue(value));
        } else {
            return new SingleResult(type, value);
        }
    }

    private List<ParameterResult> createCollectionResultValue(String value) {
        if (value == null || value.isEmpty()) {
            return new LinkedList<>();
        }
        List<ParameterResult> result = null;
        CharacterIterator it = new StringCharacterIterator(value);
        char c = it.current();
        while (c != CharacterIterator.DONE) {
            if (c == '[') {
                try {
                    result = parseCollection(it);
                } catch (Exception e) {
                    LOGGER.warn(CANNOT_PARSE_ERROR + value);
                    return null;
                }
                c = it.next();
            } else {
                LOGGER.warn(CANNOT_PARSE_ERROR + value);
                return null;
            }
        }
        return result;
    }

    private List<ParameterResult> parseCollection(CharacterIterator it) throws Exception {
        List<ParameterResult> result = new LinkedList<>();
        char c = it.next();
        while (c != CharacterIterator.DONE) {
            switch (c) {
                case ']':
                    it.next();
                    return result;
                case '[':
                    try {
                        List<ParameterResult> subCollection = parseCollection(it);
                        result.add(new CollectiveResult(DataType.COLLECTION_T, subCollection));
                        c = it.current();
                    } catch (Exception e) {
                        throw new Exception("CANNOT_PARSE_ERROR");
                    }
                    break;
                case '(':
                    try {
                        ParameterResult element = parseElement(it);
                        result.add(element);
                        c = it.next();
                    } catch (Exception e) {
                        throw new Exception("CANNOT_PARSE_ERROR");
                    }
                    break;
                case ',':
                    // Ignore
                    c = it.next();
                    break;
                default:
                    throw new Exception("CANNOT_PARSE_ERROR");
            }
        }
        return result;
    }

    private SingleResult parseElement(CharacterIterator it) throws Exception {
        char c;
        StringBuilder typeSB = new StringBuilder();
        StringBuilder valueSB = new StringBuilder();
        StringBuilder appender = typeSB;
        while ((c = it.next()) != CharacterIterator.DONE) {
            switch (c) {
                case ')':
                    DataType type = DataType.values()[Integer.parseInt(typeSB.toString())];
                    String value = valueSB.toString();
                    if (value.equalsIgnoreCase("null")) {
                        value = null;
                    }
                    return new SingleResult(type, value);
                case ',':
                    appender = valueSB;
                    break;
                default:
                    appender.append(c);
            }
        }
        throw new Exception("CANNOT_PARSE_ERROR");
    }

    /**
     * Returns the exitValue of the task (null if it has not ended yet).
     *
     * @return Exit value
     */
    public Integer getExitValue() {
        return this.exitValue;
    }

    /**
     * Returns the updated parameters (for testing purposes).
     *
     * @return Updated parameters
     */
    public List<ParameterResult> getResults() {
        return this.results;
    }

    /**
     * Returns the number of parameters of the task.
     *
     * @return Number of parameters
     */
    public int getNumParameters() {
        return this.results.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExternalTaskStatus [ ");

        sb.append("ExitValue = ").append(this.exitValue).append(", ");
        sb.append("NumParameters = ").append(getNumParameters()).append(", ");
        sb.append("Parameter = [");
        for (ParameterResult r : this.results) {
            sb.append(r.toString()).append(" ");
        }
        sb.append("]");

        sb.append(" ]");

        return sb.toString();
    }
}
