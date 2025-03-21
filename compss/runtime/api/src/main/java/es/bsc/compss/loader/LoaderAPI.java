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
package es.bsc.compss.loader;

import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.parameter.Direction;


public interface LoaderAPI {

    /**
     * Checks if a file has been accessed by the runtime.
     * 
     * @param appId Id of the application checking the file access
     * @param fileName File.
     * @return True if accessed.
     */
    public boolean isFileAccessed(Long appId, String fileName);

    /**
     * Returns the renaming of the file version opened.
     * 
     * @param appId Id of the application openning the file
     * @param fileName File.
     * @param mode Access mode.
     * @return Renaming of the current file version.
     */
    public String openFile(Long appId, String fileName, Direction mode);

    /**
     * Closes the given file {@code fileName}.
     * 
     * @param appId Id of the application closing the file
     * @param fileName File version name.
     * @param mode Access mode.
     */
    public void closeFile(Long appId, String fileName, Direction mode);

    /**
     * Deletes the specified version of a file.
     * 
     * @param appId Application id.
     * @param fileName File version name.
     * @return {@code true} if the file has been erased, {@code false} otherwise.
     */
    public boolean deleteFile(Long appId, String fileName);

    /**
     * Retrieves the last version of file with its original name.
     * 
     * @param appId Application id.
     * @param fileName File name.
     */
    public void getFile(Long appId, String fileName);

    /**
     * Bind the last known version of an object in the runtime system with another dataId.
     * 
     * @param appId Id of the application accessing the object
     * @param o Object.
     * @param hashCode Object hashcode.
     * @param dataId data to bind the object
     * @return {@literal true} if the data was bound to a previous version; {@literal false} otherwise.
     */
    public boolean bindExistingVersionToData(Long appId, Object o, Integer hashCode, String dataId);

    /**
     * Returns a copy of the last version of the given object {@code o}.
     * 
     * @param appId Id of the application accessing the object
     * @param o Object.
     * @param hashCode Object hashcode.
     * @param destDir Destination directory for serialization.
     * @return In-memory copy of the last version of the given object.
     */
    public Object getObject(Long appId, Object o, int hashCode, String destDir);

    /**
     * Serializes the given object {@code o} to the given path {@code destDir}.
     * 
     * @param o Object.
     * @param hashCode Object hashcode.
     * @param destDir Destination directory.
     */
    public void serializeObject(Object o, int hashCode, String destDir);

    /**
     * Returns the Object Registry instance.
     * 
     * @return The Object Registry instance.
     */
    public ObjectRegistry getObjectRegistry();

    /**
     * Returns the Stream Registry instance.
     * 
     * @return The Stream Registry instance.
     */
    public StreamRegistry getStreamRegistry();

    /**
     * Associates a new Object Registry.
     * 
     * @param oReg Object Registry.
     */
    public void setObjectRegistry(ObjectRegistry oReg);

    /**
     * Associates a new Stream Registry instance.
     * 
     * @param sReg Stream Registry.
     */
    public void setStreamRegistry(StreamRegistry sReg);

    /**
     * Returns the directory where to store temporary files.
     * 
     * @return The directory where to store temporary files.
     */
    public String getTempDir();

    /**
     * Removes the given object {@code o}.
     * 
     * @param appId Application Id.
     * @param o Object.
     * @param hashcode Object hashcode.
     */
    public void removeObject(Long appId, Object o, int hashcode);

    /**
     * Creates a new task group.
     *
     * @param groupName Group name.
     * @param implicitBarrier Flag stating if the group has to perform a barrier.
     * @param appId Application Id.
     */
    public void openTaskGroup(String groupName, boolean implicitBarrier, Long appId);

    /**
     * Closes an existing task group.
     *
     * @param groupName Group name.
     * @param appId Application Id.
     */
    public void closeTaskGroup(String groupName, Long appId);

}
