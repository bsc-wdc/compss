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


public abstract class DataParams {

    private final Application app;


    public abstract String getDescription();

    public abstract DataInfo createDataInfo();

    public abstract DataInfo getDataInfo();

    public abstract DataInfo removeDataInfo();

    public abstract Integer getDataId();

    /**
     * Deletes the local instance of the data.
     * 
     * @throws Exception An error arised during the deletion
     */
    public abstract void deleteLocal() throws Exception;

    public DataParams(Application app) {
        this.app = app;
    }

    public Application getApp() {
        return app;
    }

}
