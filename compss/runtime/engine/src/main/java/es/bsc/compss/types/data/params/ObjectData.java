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
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.ObjectInfo;


public class ObjectData extends DataParams {

    protected final int code;


    /**
     * Constructs a new DataParams for an object.
     *
     * @param app Application accessing the object
     * @param code code identifying the object
     */
    public ObjectData(Application app, int code) {
        super(app);
        this.code = code;
    }

    @Override
    public Integer getDataId() {
        Application app = this.getApp();
        return app.getObjectDataId(code);
    }

    @Override
    public String getDescription() {
        return "object with code " + code;
    }

    @Override
    public DataInfo createDataInfo() {
        DataInfo oInfo = new ObjectInfo(this);
        Application app = this.getApp();
        app.registerObjectData(code, oInfo);
        return oInfo;
    }

    @Override
    public DataInfo getDataInfo() {
        Application app = this.getApp();
        return app.getObjectData(code);
    }

    @Override
    public DataInfo removeDataInfo() {
        Application app = this.getApp();
        return app.removeObjectData(code);
    }

    public final int getCode() {
        return this.code;
    }

    @Override
    public void deleteLocal() throws Exception {
        // No need to do anything to remove the local instance
    }
}
