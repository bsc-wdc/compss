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
package es.bsc.compss.agent.rest.types;

import jakarta.xml.bind.annotation.XmlAttribute;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlElements;
import jakarta.xml.bind.annotation.XmlRootElement;
import java.lang.reflect.Array;
import storage.StorageException;
import storage.StorageItf;


/**
 * This class contains all the information to pass a parameter value through the REST Agent interface in XML format.
 */
public abstract class ApplicationParameterValue {

    /**
     * Constructs a new ApplicationParameterValue out of the object {@code obj}.
     *
     * @param obj value contained on the ParameterValue
     * @return ApplicationParameterValue constructed with the value passed in as parameter.
     */
    public static final ApplicationParameterValue createParameterValue(Object obj) {
        if (obj.getClass().isArray()) {
            return new ArrayParameter(obj);
        } else {
            return new ElementParameter(obj);
        }
    }


    private int paramId;


    @XmlAttribute
    public int getParamId() {
        return paramId;
    }

    public void setParamId(int paramId) {
        this.paramId = paramId;
    }

    public abstract Object getValue() throws ClassNotFoundException;

    public abstract Object getContent() throws Exception;

    public abstract String getType();


    @XmlRootElement(name = "ArrayParameter")
    public static class ArrayParameter extends ApplicationParameterValue {

        private String componentClassname;
        private ApplicationParameterValue[] elements;


        public ArrayParameter() {

        }

        @XmlElementWrapper(name = "values")
        @XmlElements({
            @XmlElement(name = "array", type = ApplicationParameterValue.ArrayParameter.class, required = false),
            @XmlElement(name = "element", type = ApplicationParameterValue.ElementParameter.class, required = false), })
        public ApplicationParameterValue[] getElements() {
            return elements;
        }

        public void setElements(ApplicationParameterValue[] elements) {
            this.elements = elements;
        }

        public String getComponentClassname() {
            return componentClassname;
        }

        public void setComponentClassname(String componentClass) {
            this.componentClassname = componentClass;
        }

        private ArrayParameter(Object o) {
            Class<?> componentClass = o.getClass().getComponentType();
            componentClassname = componentClass.getCanonicalName();
            int numElements = Array.getLength(o);
            elements = new ApplicationParameterValue[numElements];
            for (int elementId = 0; elementId < numElements; elementId++) {
                elements[elementId] = ApplicationParameterValue.createParameterValue(Array.get(o, elementId));
                elements[elementId].paramId = elementId;
            }
        }

        @Override
        public Object getValue() throws ClassNotFoundException {
            Class<?> componentClass = Class.forName(componentClassname);
            int numElements = elements.length;
            Object array = Array.newInstance(componentClass, numElements);
            for (ApplicationParameterValue element : elements) {
                int position = element.getParamId();
                Object value = element.getValue();
                Array.set(array, position, value);
            }
            return array;
        }

        @Override
        public Object getContent() throws ClassNotFoundException {
            return getValue();
        }

        @Override
        public String getType() {
            if (elements.length > 0) {
                return elements[0].getType() + "[";
            } else {
                return componentClassname + "[";
            }
        }

    }

    @XmlRootElement(name = "ElementParameter")
    public static class ElementParameter extends ApplicationParameterValue {

        private Object value;
        private String className;


        public ElementParameter() {
        }

        private ElementParameter(Object o) {
            value = o;
            className = o.getClass().getCanonicalName();
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getClassName() {
            return className;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        @Override
        public Object getValue() {
            return value;
        }

        @Override
        public Object getContent() throws StorageException {
            Object val;
            if (className.compareTo(value.getClass().getCanonicalName()) == 0) {
                val = value;
            } else {
                val = StorageItf.getByID((String) value);
            }
            return val;
        }

        @Override
        public String getType() {
            return className;
        }

    }
}
