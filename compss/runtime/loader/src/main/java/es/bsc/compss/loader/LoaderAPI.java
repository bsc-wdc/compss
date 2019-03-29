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
package es.bsc.compss.loader;

import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.parameter.Direction;


public interface LoaderAPI {

    /**
     * Returns the renaming of the file version opened.
     */
    public String openFile(String fileName, Direction mode);

    /**
     * Close the file version opened.
     */
    public void closeFile(String fileName, Direction mode);

    /**
     * Deletes the specified version of a file.
     */
    public boolean deleteFile(String fileName);

    /**
     * Returns last version of file with its original name.
     */
    public void getFile(Long appId, String fileName);

    /**
     * Returns a copy of the last object version.
     */
    public Object getObject(Object o, int hashCode, String destDir);

    /**
     * Serializes the given object.
     */
    public void serializeObject(Object o, int hashCode, String destDir);

    /**
     * Gets the object Registry instance.
     */
    public ObjectRegistry getObjectRegistry();

    /**
     * Gets the stream Registry instance.
     */
    public StreamRegistry getStreamRegistry();

    /**
     * Sets the object Registry instance.
     */
    public void setObjectRegistry(ObjectRegistry oReg);

    /**
     * Sets the object Registry instance.
     */
    public void setStreamRegistry(StreamRegistry oReg);

    /**
     * Returns the directory where to store temporary files.
     */
    public String getTempDir();

    /**
     * Removes the TODO.
     */
    public void removeObject(Object o, int hashcode);

}
