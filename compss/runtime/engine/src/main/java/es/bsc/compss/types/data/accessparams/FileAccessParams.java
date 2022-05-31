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
package es.bsc.compss.types.data.accessparams;

import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.location.DataLocation;


public class FileAccessParams extends AccessParams {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    private DataLocation loc;


    /**
     * Creates a new FileAccessParams instance with the given mode {@code mode} and for the given file location
     * {@code loc}.
     *
     * @param app Id of the application accessing the file.
     * @param mode Access mode.
     * @param loc File location.
     */
    public FileAccessParams(Application app, AccessMode mode, DataLocation loc) {
        super(app, mode);
        this.loc = loc;
    }

    /**
     * Returns the file location.
     * 
     * @return The file location.
     */
    public DataLocation getLocation() {
        return this.loc;
    }

    @Override
    public DataAccessId registerAccess(DataInfoProvider dip) {
        return dip.registerFileAccess(this.app, this.mode, this.loc);
    }

    @Override
    public void registerAccessCompletion(DataInfoProvider dip) {
        dip.finishFileAccess(this.app, this.mode, this.loc);
    }

    @Override
    public String toString() {
        return "[" + this.app + ", " + this.mode + " ," + this.loc + "]";
    }
}
