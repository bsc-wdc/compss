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

import es.bsc.compss.types.data.info.CollectionInfo;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;


public interface DataOwner {

    /**
     * Stores the relation between a file and the corresponding dataInfo.
     *
     * @param locationKey file location
     * @param di data registered by the application
     */
    void registerFileData(String locationKey, FileInfo di);

    /**
     * Returns the Data related to a file.
     *
     * @param locationKey file location
     * @return data related to the file
     */
    FileInfo getFileData(String locationKey);

    /**
     * Removes any data association related to file location.
     *
     * @param locationKey file location
     * @return data Id related to the file
     * @throws ValueUnawareRuntimeException the application is not aware of the data
     */
    FileInfo removeFileData(String locationKey) throws ValueUnawareRuntimeException;

    /**
     * Stores the relation between an object and the corresponding dataInfo.
     *
     * @param code hashcode of the object
     * @param di data registered by the application
     */
    void registerObjectData(int code, DataInfo di);

    /**
     * Returns the Data related to an object.
     *
     * @param code hashcode of the object
     * @return data related to the object
     */
    DataInfo getObjectData(int code);

    /**
     * Removes any data association related to an object.
     *
     * @param code hashcode of the object
     * @return data Id related to the object
     * @throws ValueUnawareRuntimeException the application is not aware of the data
     */
    DataInfo removeObjectData(int code) throws ValueUnawareRuntimeException;

    /**
     * Stores the relation between a collection and the corresponding dataInfo.
     *
     * @param collectionId Id of the collection
     * @param di data registered by the application
     */
    void registerCollectionData(String collectionId, CollectionInfo di);

    /**
     * Returns the Data related to a collection.
     *
     * @param collectionId Id of the collection
     * @return data related to the file
     */
    CollectionInfo getCollectionData(String collectionId);

    /**
     * Removes any data association related to a collection.
     *
     * @param collectionId Id of the collection
     * @return data Id related to the file
     * @throws ValueUnawareRuntimeException the application is not aware of the data
     */
    CollectionInfo removeCollectionData(String collectionId) throws ValueUnawareRuntimeException;

}
