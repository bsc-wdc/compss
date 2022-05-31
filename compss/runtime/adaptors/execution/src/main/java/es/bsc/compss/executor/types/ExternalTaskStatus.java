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
package es.bsc.compss.executor.types;

import es.bsc.compss.invokers.types.TypeValuePair;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores the task status information for bindings.
 */
public class ExternalTaskStatus {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private final Integer exitValue;
    private final List<TypeValuePair> updatedParameters;


    private static class CollectionBean {

        // Necessary to parse the collection string representation
        int token;
        Object obj;


        public CollectionBean(int token, Object o) {
            super();
            this.token = token;
            this.obj = o;
        }
    }


    /**
     * Creates a new task status instance with exitValue.
     *
     * @param exitValue Exit Value
     */
    public ExternalTaskStatus(Integer exitValue) {
        this.exitValue = exitValue;
        this.updatedParameters = new LinkedList<>();
    }

    /**
     * Creates a new task status instance with exitValue.
     *
     * @param line task status content as string array
     */
    public ExternalTaskStatus(String[] line) {
        this.updatedParameters = new LinkedList<>();

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
    public List<TypeValuePair> getUpdatedParameters() {
        return this.updatedParameters;
    }

    /**
     * Returns the number of parameters of the task.
     *
     * @return Number of parameters
     */
    public int getNumParameters() {
        return this.updatedParameters.size();
    }

    /**
     * Returns the i-th parameter type. Null if i is out of the parameters range.
     *
     * @param i Parameter ordinal
     * @return
     */
    public DataType getParameterType(int i) {
        if (i >= 0 && i < this.updatedParameters.size()) {
            return this.updatedParameters.get(i).getUpdatedParameterType();
        }
        return null;
    }

    /**
     * Returns the i-th parameter value. Null if i is out of the parameters range
     *
     * @param i Parameter ordinal
     * @return
     */
    public String getParameterValue(int i) {
        if (i >= 0 && i < this.updatedParameters.size()) {
            return (String) this.updatedParameters.get(i).getUpdatedParameterValue();
        }
        return null;
    }

    /**
     * Returns the i-th parameter collection value. Null if i is out of the parameters range.
     *
     * @param i Parameter ordinal
     * @return
     */
    public LinkedList<Object> getParameterValues(int i) {
        if (i >= 0 && i < this.updatedParameters.size()) {
            return this.updatedParameters.get(i).getUpdatedParameterValues();
        }
        return null;
    }

    /**
     * Parses a collection from string representation. (((type, value), (type, value)), ((type, value), (type, value)))
     * 
     * @param collection String Collection representation
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<Object> buildCollectionList(String collection) {
        Deque<Object> stack = new ArrayDeque<Object>();
        char[] chararray = collection.toCharArray();
        String temp = "";
        for (int i = 0; i < chararray.length; i++) {
            if (chararray[i] == ')') {
                if (!temp.equals("")) {
                    stack.push(new CollectionBean(0, temp));
                    temp = "";
                }
                List<Object> tmplist = new ArrayList<>();
                while (true) {
                    Object object = stack.pop();
                    if (object instanceof CollectionBean) {
                        CollectionBean b = (CollectionBean) object;
                        if (b.token == 1) {
                            break;
                        }
                        tmplist.add(b.obj);
                    } else {
                        tmplist.add(object);
                        if (stack.isEmpty()) {
                            break;
                        }
                    }
                }
                Collections.reverse(tmplist);
                stack.push(tmplist);
            } else {
                if (chararray[i] == '(') {
                    stack.push(new CollectionBean(1, Character.toString(chararray[i])));
                } else if (chararray[i] == ',') {
                    if (!temp.equals("")) {
                        stack.push(new CollectionBean(0, temp));
                        temp = "";
                    }
                } else {
                    temp = temp + Character.toString(chararray[i]);
                }
            }
        }
        return (List<Object>) stack.pop();
    }

    /**
     * Converts the list representation of a collection into a Type value pair recursively.
     * 
     * @param collection String Collection representation
     */
    @SuppressWarnings("unchecked")
    public List<Object> buildCollectionParameter(List<Object> collection) {
        // Recursive call to add parameter if detected collection (length == 2 is a single element)
        List<Object> param = new LinkedList<Object>();
        for (Object element : collection) {
            List<Object> subParam = (List<Object>) element;
            if (subParam.size() == 2 && subParam.get(0) instanceof String) {
                // Simple element within collection
                DataType subElemType = DataType.values()[Integer.parseInt((String) subParam.get(0))];
                String value = (String) subParam.get(1);
                if (value.compareTo("null") != 0) {
                    param.add(new TypeValuePair(subElemType, (String) subParam.get(1)));
                } else {
                    param.add(new TypeValuePair(subElemType, null));
                }
            } else {
                // It is a collection in the collection
                param.add(buildCollectionParameter(subParam));
            }
        }
        return param;
    }

    /**
     * Adds a new parameter.
     *
     * @param type Parameter Type
     * @param value Parameter Value
     */
    public void addParameter(DataType type, String value) {
        if (type == DataType.COLLECTION_T && value != null) {
            TypeValuePair collectionParameter = new TypeValuePair(type);
            value = value.replace("[", "(");
            value = value.replace("]", ")");
            List<Object> collection = buildCollectionList(value);
            List<Object> parameterValue = buildCollectionParameter(collection);
            collectionParameter.setUpdatedParameterValue((LinkedList<Object>) parameterValue);
            this.updatedParameters.add(collectionParameter);
        } else {
            this.updatedParameters.add(new TypeValuePair(type, value));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExternalTaskStatus [ ");

        sb.append("ExitValue = ").append(this.exitValue).append(", ");
        sb.append("NumParameters = ").append(getNumParameters()).append(", ");
        sb.append("ParameterTypes = [");
        for (TypeValuePair tvp : this.updatedParameters) {
            sb.append(tvp.getUpdatedParameterType().ordinal()).append(" ");
        }
        sb.append("], ");
        sb.append("ParameterValues = [");
        for (TypeValuePair tvp : this.updatedParameters) {
            sb.append((String) tvp.getUpdatedParameterValue()).append(" ");
        }
        sb.append("]");

        sb.append(" ]");

        return sb.toString();
    }

}
