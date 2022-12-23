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
import es.bsc.compss.types.data.CollectionInfo;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.FileInfo;
import es.bsc.compss.types.data.ObjectInfo;
import es.bsc.compss.types.data.location.DataLocation;


public abstract class DataParams {

    private final Application app;


    public abstract String getDescription();

    public abstract Integer getDataId(DataInfoProvider dip);

    public abstract DataInfo createDataInfo(DataInfoProvider dip);

    public DataParams(Application app) {
        this.app = app;
    }

    public Application getApp() {
        return app;
    }

    /**
     * Registers a new data into the system.
     * 
     * @param dip data repository
     * @return new Registered data
     */
    public DataInfo registerData(DataInfoProvider dip) {
        DataInfo dInfo;
        dInfo = createDataInfo(dip);
        app.addData(dInfo);
        dip.registerData(dInfo);
        return dInfo;
    }


    public static class FileData extends DataParams {

        private final String locKey;
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
        public DataInfo createDataInfo(DataInfoProvider dip) {
            Application app = this.getApp();
            DataInfo dInfo = new FileInfo(app, loc);
            app.registerFileData(this.locKey, dInfo);
            return dInfo;
        }

        @Override
        public Integer getDataId(DataInfoProvider dip) {
            Application app = this.getApp();
            String locationKey = loc.getLocationKey();
            return app.getFileDataId(locationKey);
        }

    }

    public static class ObjectData extends DataParams {

        private final int code;


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
        public Integer getDataId(DataInfoProvider dip) {
            return dip.getObjectDataId(code);
        }

        @Override
        public String getDescription() {
            return "object with code " + code;
        }

        @Override
        public DataInfo createDataInfo(DataInfoProvider dip) {
            Application app = this.getApp();
            DataInfo oInfo = new ObjectInfo(app, code);
            dip.registerObjectDataId(code, oInfo.getDataId());
            return oInfo;
        }

    }

    public static class ExternalPSCObjectData extends ObjectData {

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

    public static class BindingObjectData extends ObjectData {

        /**
         * Constructs a new DataParams for a binding object.
         *
         * @param app Application accessing the object
         * @param code code identifying the object
         */
        public BindingObjectData(Application app, int code) {
            super(app, code);
        }

        @Override
        public String getDescription() {
            return "binding " + super.getDescription();
        }

    }

    public static class CollectionData extends DataParams {

        private final String collectionId;


        /**
         * Constructs a new DataParams for a collection.
         * 
         * @param app Application accessing the collection
         * @param collectionId Id of the collection
         */
        public CollectionData(Application app, String collectionId) {
            super(app);
            this.collectionId = collectionId;
        }

        @Override
        public String getDescription() {
            return "collection  " + this.collectionId;
        }

        @Override
        public Integer getDataId(DataInfoProvider dip) {
            return dip.getCollectionDataId(collectionId);
        }

        @Override
        public DataInfo createDataInfo(DataInfoProvider dip) {
            Application app = this.getApp();
            DataInfo oInfo = new CollectionInfo(app, collectionId);
            dip.registerCollectionDataId(collectionId, oInfo.getDataId());
            return oInfo;
        }
    }
}
