/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;


public class NewVersionSameValueRequest extends APRequest {

    private final String rRenaming;
    private final String wRenaming;


    /**
     * Creates a new request to create a new version of a given renaming with the same value.
     * 
     * @param rRenaming Original renaming.
     * @param wRenaming New renaming.
     */
    public NewVersionSameValueRequest(String rRenaming, String wRenaming) {
        super();
        this.rRenaming = rRenaming;
        this.wRenaming = wRenaming;
    }

    /**
     * Returns the original renaming.
     * 
     * @return The original renaming.
     */
    public String getrRenaming() {
        return this.rRenaming;
    }

    /**
     * Returns the new renaming.
     * 
     * @return The new renaming.
     */
    public String getwRenaming() {
        return this.wRenaming;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.newVersionSameValue(this.rRenaming, this.wRenaming);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.NEW_VERSION_SAME_VALUE;
    }

}
