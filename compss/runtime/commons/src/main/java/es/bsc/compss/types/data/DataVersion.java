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
package es.bsc.compss.types.data;

public class DataVersion {

    private final DataInstanceId dataInstanceId;
    private int readers;
    private int writters;
    private boolean toDelete;


    public DataVersion(int dataId, int versionId) {
        this.readers = 0;
        this.dataInstanceId = new DataInstanceId(dataId, versionId);
        this.writters = 0;
        this.toDelete = false;
    }

    public DataInstanceId getDataInstanceId() {
        return this.dataInstanceId;
    }

    public void willBeRead() {
        this.readers++;
    }

    public void willBeWritten() {
        this.writters++;
    }

    public boolean hasPendingLectures() {
        return this.readers > 0;
    }

    public boolean isOnlyReader() {
        return readers > 1;
    }

    public boolean hasBeenRead() {
        this.readers--;
        return checkDeletion();
    }

    public boolean hasBeenWritten() {
        this.writters--;
        return checkDeletion();
    }

    public boolean delete() {
        this.toDelete = true;
        if (this.readers == 0 && this.writters == 0) {
            return true;
        }
        return false;
    }

    private boolean checkDeletion() {
        if (this.toDelete // deletion requested
                && this.writters == 0 // version has been generated
                && this.readers == 0 // version has been read
        ) {
            return true;
        }
        return false;
    }

    public boolean isToDelete() {
        return this.toDelete;
    }

}
