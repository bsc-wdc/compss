/*
 *  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data.params;

import es.bsc.compss.types.Application;


public class ExternalPSCObjectData extends ObjectData {

    /**
     * Constructs a new DataParams for a binding object.
     *
     * @param app Application accessing the object
     * @param code code identifying the object
     */
    public ExternalPSCObjectData(Application app, int code) {
        super(app, code);
    }

    @Override
    public String getDescription() {
        return "external " + super.getDescription();
    }

}
