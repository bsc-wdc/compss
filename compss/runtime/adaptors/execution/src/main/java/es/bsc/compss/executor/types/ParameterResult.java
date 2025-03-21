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

import es.bsc.compss.types.annotations.parameter.DataType;
import java.util.List;


public abstract class ParameterResult {

    private final DataType type;


    /**
     * Creates a new empty Type Value Pair instance. Used for collections
     */
    public ParameterResult(DataType type) {
        this.type = type;

    }

    /**
     * Parameter type getter.
     *
     * @return The parameter type.
     */
    public final DataType getType() {
        return type;
    }

    /**
     * Returns whether the return is for a single element or a collection of parameters.
     *
     * @return {@literal true} when the result collects several parameter results; {@literal false} otherwise.
     */
    public abstract boolean isCollective();


    public static class SingleResult extends ParameterResult {

        private final String value;


        public SingleResult(DataType type, String value) {
            super(type);
            this.value = value;
        }

        @Override
        public boolean isCollective() {
            return false;
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "(" + this.getType().ordinal() + " " + this.value + ")";
        }
    }

    public static class CollectiveResult extends ParameterResult {

        public final List<ParameterResult> elements;


        public CollectiveResult(DataType type, List<ParameterResult> elements) {
            super(type);
            this.elements = elements;
        }

        @Override
        public boolean isCollective() {
            return true;
        }

        public List<ParameterResult> getElements() {
            return elements;
        }

        public int size() {
            return elements.size();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("(" + this.getType().ordinal() + " [");
            for (ParameterResult r : this.elements) {
                sb.append(r.toString()).append(" ");
            }
            sb.append("])");
            return sb.toString();
        }
    }
}
