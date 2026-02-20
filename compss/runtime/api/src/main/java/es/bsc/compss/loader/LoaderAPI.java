/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.types.annotations.parameter.Direction;


public interface LoaderAPI {

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
     * Retrieves the last version of file with its original name.
     * 
     * @param appId Application id.
     * @param fileName File name.
     */
    public void getFile(Long appId, String fileName);

    /**
     * Returns the Stream Registry instance.
     * 
     * @return The Stream Registry instance.
     */
    public StreamRegistry getStreamRegistry();

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

}
