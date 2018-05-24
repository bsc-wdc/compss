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

import java.util.LinkedList;

import es.bsc.compss.comm.Comm;

public class ObjectInfo extends DataInfo {

    // Hash code of the object

    private int code;


    public ObjectInfo(int code) {
        super();
        this.code = code;
    }

    public int getCode() {
        return code;
    }
    
    @Override
    public LinkedList<Integer> delete() {
        LinkedList<Integer> renamings_to_delete = new LinkedList<Integer>(); //Renamings that we can erase from memory (or provoke that are)
        
        if (deletionBlocks > 0) {
            pendingDeletions.addAll(versions.values());
        } else {
            LinkedList<Integer> removedVersions = new LinkedList<>();
            for (DataVersion version : versions.values()) {
                if (version.delete()) {
                	//Not sure if this hashCode is the one we want, but we always can take
                	//the string of the renaming and extract the hashCode afterwards
                	renamings_to_delete.add(version.getDataInstanceId().hashCode());
                	Comm.removeData(version.getDataInstanceId().getRenaming());
                    removedVersions.add(version.getDataInstanceId().getVersionId());
                }
            }
            for (int versionId : removedVersions) {
                versions.remove(versionId);
            }
        }
        
        return renamings_to_delete; //From 0 to N renamings_to_delete
    }

}
