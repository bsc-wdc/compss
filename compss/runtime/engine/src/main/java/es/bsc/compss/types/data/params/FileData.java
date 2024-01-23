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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.util.FileOpsManager;

import java.io.File;


public class FileData extends DataParams {

    protected final String locKey;
    private final DataLocation loc;


    /**
     * Constructs a new DataParams for a file.
     *
     * @param app Application accessing the file
     * @param loc location of the file
     */
    public FileData(Application app, DataLocation loc) {
        super(app);
        this.loc = loc;
        this.locKey = loc.getLocationKey();
    }

    @Override
    public String getDescription() {
        return "file " + this.locKey;
    }

    @Override
    public Integer getDataId() {
        Application app = this.getApp();
        String locationKey = loc.getLocationKey();
        return app.getFileDataId(locationKey);
    }

    @Override
    public DataInfo createDataInfo() {
        Application app = this.getApp();
        DataInfo dInfo = new FileInfo(this);
        app.registerFileData(this.locKey, dInfo);
        return dInfo;
    }

    @Override
    public DataInfo getDataInfo() {
        Application app = this.getApp();
        String locationKey = loc.getLocationKey();
        return app.getFileData(locationKey);
    }

    @Override
    public DataInfo removeDataInfo() {
        Application app = this.getApp();
        String locationKey = loc.getLocationKey();
        return app.removeFileData(locationKey);
    }

    public DataLocation getLocation() {
        return this.loc;
    }

    @Override
    public void deleteLocal() throws Exception {
        String filePath = getLocation().getURIInHost(Comm.getAppHost()).getPath();
        File f = new File(filePath);
        FileOpsManager.deleteSync(f);
    }

}
