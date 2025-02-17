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
package es.bsc.compss.types.data;

import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.location.DataLocation;

import java.io.File;


public class ResultFile {

    private final FileInfo fi;
    private LogicalData data;


    /**
     * Creates a new ResultFile instance for data {@code fId} from logicalData {@code data}.
     *
     * @param fi Associated FileInfo
     * @param data LogicalData to be fetched.
     */
    public ResultFile(FileInfo fi, LogicalData data) {
        this.fi = fi;
        this.data = data;
    }

    public LogicalData getData() {
        return data;
    }

    public FileInfo getFileInfo() {
        return this.fi;
    }

    /**
     * Returns the original file location.
     *
     * @return The original file location.
     */
    public DataLocation getOriginalLocation() {
        return this.fi.getOriginalLocation();
    }

    @Override
    public String toString() {
        return this.fi.getOriginalLocation().toString();
    }

}
