/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
     * Returns the renaming of the file version opened
     *
     * @param fileName
     * @param mode
     * @return
     */
    public String openFile(String fileName, Direction mode);

    /**
     * Returns the renaming of the last file version just transferred
     *
     * @param fileName
     * @param destDir
     * @return
     */
    public String getFile(String fileName, String destDir);

    /**
     * Returns a copy of the last object version
     *
     * @param o
     * @param hashCode
     * @param destDir
     * @return
     */
    public Object getObject(Object o, int hashCode, String destDir);

    /**
     * Serializes the given object
     *
     * @param o
     * @param hashCode
     * @param destDir
     */
    public void serializeObject(Object o, int hashCode, String destDir);

    /**
     * Gets the object Registry instance
     *
     * @return
     */
    public ObjectRegistry getObjectRegistry();

    /**
     * Gets the stream Registry instance
     *
     * @return
     */
    public StreamRegistry getStreamRegistry();

    /**
     * Sets the object Registry instance
     *
     * @param oReg
     */
    public void setObjectRegistry(ObjectRegistry oReg);

    /**
     * Sets the object Registry instance
     *
     * @param oReg
     */
    public void setStreamRegistry(StreamRegistry oReg);

    /**
     * Returns the directory where to store temporary files
     *
     * @return
     */
    public String getTempDir();

    /**
     *
     * Removes the
     *
     * @param o
     * @param hashcode
     */
    public void removeObject(Object o, int hashcode);

}
