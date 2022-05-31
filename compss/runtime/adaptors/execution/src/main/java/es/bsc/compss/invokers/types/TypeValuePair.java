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

package es.bsc.compss.invokers.types;

import es.bsc.compss.types.annotations.parameter.DataType;

import java.util.LinkedList;
import java.util.List;


public class TypeValuePair {

    private DataType updatedParameterType;
    // If type is not collection, the next list contains only one element of String type (usually null or pscoid)
    private List<Object> updatedParameterValues;


    /**
     * Creates a new empty Type Value Pair instance. Used for collections
     */
    public TypeValuePair(DataType type) {
        this.updatedParameterType = type;
        this.updatedParameterValues = new LinkedList<Object>();
    }

    /**
     * Creates a new Type Value Pair instance.
     *
     * @param type Parameter type
     * @param value Parameter value
     */
    public TypeValuePair(DataType type, String value) {
        this.updatedParameterType = type;
        this.updatedParameterValues = new LinkedList<Object>();
        this.updatedParameterValues.add(value);
    }

    /**
     * Parameter type getter.
     * 
     * @return The parameter type.
     */
    public DataType getUpdatedParameterType() {
        return updatedParameterType;
    }

    /**
     * Parameter type setter.
     * 
     * @param updatedParameterType Parameter type
     */
    public void setUpdatedParameterType(DataType updatedParameterType) {
        this.updatedParameterType = updatedParameterType;
    }

    /**
     * Get the firsts element of the LinkedList.
     * 
     * @return the first element. Null if not a string.
     */
    public Object getUpdatedParameterValue() {
        if (this.updatedParameterValues != null) {
            return this.updatedParameterValues.get(0);
        } else {
            return null;
        }
    }

    /**
     * Parameter values getter.
     * 
     * @return The parameter values.
     */
    public LinkedList<Object> getUpdatedParameterValues() {
        return (LinkedList<Object>) this.updatedParameterValues;
    }

    /**
     * Parameter value setter from string. WARNING: Resets the existing LinkedList instantiating a new one with the
     * given parameter value.
     * 
     * @param updatedParameterValues Parameter value as String
     */
    public void setUpdatedParameterValue(String updatedParameterValues) {
        this.updatedParameterValues = new LinkedList<Object>();
        this.updatedParameterValues.add(updatedParameterValues);
    }

    /**
     * Parameter value setter from LinkedList.
     * 
     * @param updatedParameterValues Parameter value as LinkedList
     */
    public void setUpdatedParameterValue(LinkedList<Object> updatedParameterValues) {
        this.updatedParameterValues = updatedParameterValues;
    }

}
