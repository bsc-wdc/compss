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
package es.bsc.compss.types.data;

import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;

import java.io.File;


public class ResultFile implements Comparable<ResultFile> {

    private final FileInfo fi;
    private DataInstanceId fId;
    private DataLocation originalLocation;


    /**
     * Creates a new ResultFile instance for data {@code fId} at location {@code location}.
     * 
     * @param fId Associated DataInstanceId
     * @param location Data location.
     */
    public ResultFile(FileInfo fi, DataInstanceId fId, DataLocation location) {
        this.fi = fi;
        this.fId = fId;
        this.originalLocation = location;
    }

    /**
     * Returns the associated DataInstanceId.
     * 
     * @return The associated DataInstanceId.
     */
    public DataInstanceId getFileInstanceId() {
        return this.fId;
    }

    /**
     * Returns the original file location.
     * 
     * @return The original file location.
     */
    public DataLocation getOriginalLocation() {
        return this.originalLocation;
    }

    // Comparable interface implementation
    @Override
    public int compareTo(ResultFile resFile) throws NullPointerException {
        if (resFile == null) {
            throw new NullPointerException();
        }

        // Compare file identifiers
        return this.fId.compareTo(resFile.fId);
    }

    @Override
    public String toString() {
        return this.fId.getRenaming();
    }

    public FileInfo getFileInfo() {
        return this.fi;
    }
}
