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
package es.bsc.compss.types.data.params;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.util.FileOpsManager;

import java.io.File;


public class FileData extends DataParams {

    protected final String locKey;
    private final DataLocation loc;


    /**
     * Constructs a new DataParams for a file.
     *
     * @param loc location of the file
     */
    public FileData(DataLocation loc) {
        this.loc = loc;
        this.locKey = loc.getLocationKey();
    }

    public DataLocation getLocation() {
        return this.loc;
    }

    public String getLocationKey() {
        return this.locKey;
    }

    @Override
    public String getDescription() {
        return "file " + this.locKey;
    }

    @Override
    protected DataInfo registerData(DataOwner owner) {
        DataInfo dInfo = new FileInfo(this, owner);
        return dInfo;
    }

    @Override
    public DataInfo getRegisteredData(DataOwner owner) {
        String locationKey = loc.getLocationKey();
        return owner.getFileData(locationKey);
    }

    @Override
    protected DataInfo unregisterData(DataOwner owner) throws ValueUnawareRuntimeException {
        String locationKey = loc.getLocationKey();
        return owner.removeFileData(locationKey);
    }

    @Override
    public void deleteLocal() throws Exception {
        String filePath = getLocation().getURIInHost(Comm.getAppHost()).getPath();
        File f = new File(filePath);
        FileOpsManager.deleteSync(f);
    }
}
