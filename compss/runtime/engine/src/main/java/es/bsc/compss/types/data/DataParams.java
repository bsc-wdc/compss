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
package es.bsc.compss.types.data;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.util.FileOpsManager;
import java.io.File;


public abstract class DataParams {

    private final Application app;


    public abstract String getDescription();

    public abstract DataInfo createDataInfo(DataInfoProvider dip);

    public abstract Integer getDataId(DataInfoProvider dip);

    public abstract Integer removeDataId(DataInfoProvider dip);

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


    public static class FileData extends DataParams {

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
        public DataInfo createDataInfo(DataInfoProvider dip) {
            Application app = this.getApp();
            DataInfo dInfo = new FileInfo(this);
            app.registerFileData(this.locKey, dInfo);
            return dInfo;
        }

        @Override
        public Integer getDataId(DataInfoProvider dip) {
            Application app = this.getApp();
            String locationKey = loc.getLocationKey();
            return app.getFileDataId(locationKey);
        }

        @Override
        public Integer removeDataId(DataInfoProvider dip) {
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

    public static class DirectoryData extends FileData {

        /**
         * Constructs a new DataParams for a directory.
         * 
         * @param app Application accessing the directory
         * @param loc location of the directory
         */
        public DirectoryData(Application app, DataLocation loc) {
            super(app, loc);
        }

        @Override
        public String getDescription() {
            return "directory " + this.locKey;
        }

    }

    public static class ObjectData extends DataParams {

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
        public Integer getDataId(DataInfoProvider dip) {
            Application app = this.getApp();
            return app.getObjectDataId(code);
        }

        @Override
        public String getDescription() {
            return "object with code " + code;
        }

        @Override
        public DataInfo createDataInfo(DataInfoProvider dip) {
            DataInfo oInfo = new ObjectInfo(this);
            Application app = this.getApp();
            app.registerObjectData(code, oInfo);
            return oInfo;
        }

        @Override
        public Integer removeDataId(DataInfoProvider dip) {
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

    public static class StreamData extends ObjectData {

        public StreamData(Application app, int code) {
            super(app, code);

        }

        @Override
        public String getDescription() {
            return "stream " + code;
        }

        @Override
        public DataInfo createDataInfo(DataInfoProvider dip) {
            DataInfo sInfo = new StreamInfo(this);
            Application app = this.getApp();
            app.registerObjectData(code, sInfo);
            return sInfo;
        }

    }

    public static class ExternalStreamData extends StreamData {

        public ExternalStreamData(Application app, int code) {
            super(app, code);
        }

        @Override
        public String getDescription() {
            return "external " + super.getDescription();
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
            Application app = this.getApp();
            return app.getCollectionDataId(this.collectionId);
        }

        @Override
        public DataInfo createDataInfo(DataInfoProvider dip) {
            DataInfo cInfo = new CollectionInfo(this);
            Application app = this.getApp();
            app.registerCollectionData(this.collectionId, cInfo);
            return cInfo;
        }

        @Override
        public Integer removeDataId(DataInfoProvider dip) {
            Application app = this.getApp();
            return app.removeCollectionData(this.collectionId);
        }

        public String getCollectionId() {
            return this.collectionId;
        }

        @Override
        public void deleteLocal() throws Exception {
            // No need to do anything to remove the local instance
        }
    }
}
