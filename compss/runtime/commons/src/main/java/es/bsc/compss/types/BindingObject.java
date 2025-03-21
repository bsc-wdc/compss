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

import java.io.File;


public class BindingObject {

    private String id;
    private int type;
    private int elements;


    /**
     * Creates a new BindingObject instance.
     * 
     * @param id Binding object id.
     * @param type Binding object type.
     * @param elements Number of elements.
     */
    public BindingObject(String id, int type, int elements) {
        this.id = id;
        this.type = type;
        this.elements = elements;
    }

    /**
     * Returns the binding object id.
     * 
     * @return The binding object id.
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns the binding object name.
     * 
     * @return The binding object name.
     */
    public String getName() {
        int index = this.id.lastIndexOf(File.separator);
        if (index > 0) {
            return this.id.substring(index + 1);
        } else {
            return this.id;
        }
    }

    /**
     * Returns the binding object type.
     * 
     * @return The binding object type.
     */
    public int getType() {
        return this.type;
    }

    /**
     * Returns the binding object number of elements.
     * 
     * @return The binding object number of elements.
     */
    public int getElements() {
        return this.elements;
    }

    /**
     * Generates a new BindingObject instance from the given {@code path}.
     * 
     * @param path BindingObject path.
     * @return The BindingObject representing the given {@code path}.
     */
    public static BindingObject generate(String path) {
        String[] extObjVals = path.split("#");
        // id = extObjVals[0].substring(extObjVals[0].lastIndexOf(File.pathSeparator)+1);
        String id = extObjVals[0];
        int type = Integer.parseInt(extObjVals[1]);
        int elements = Integer.parseInt(extObjVals[2]);
        return new BindingObject(id, type, elements);
    }

    @Override
    public String toString() {
        return this.id + "#" + this.type + "#" + this.elements;
    }

}
