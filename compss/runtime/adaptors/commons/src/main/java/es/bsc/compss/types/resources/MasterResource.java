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
package es.bsc.compss.types.resources;

import java.util.Map;


public interface MasterResource extends Resource {

    public String getTempDirPath();

    public String getAppLogDirPath();

    public String getJobsDirPath();

    public String getWorkersDirPath();

    public String getWorkingDirectory();

    /*
    private boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return false;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    if (!f.delete()) {
                        return false;
                    }
                }
            }
        }

        return directory.delete();
    }

    public String getCOMPSsLogBaseDirPath() {
        return COMPSsLogBaseDirPath;
    }

    public String getWorkingDirectory() {
        return tempDirPath;
    }

    public String getUserExecutionDirPath() {
        return userExecutionDirPath;
    }

    public String getAppLogDirPath() {
        return appLogDirPath;
    }

    public String getTempDirPath() {
        return tempDirPath;
    }

    public String getJobsDirPath() {
        return jobsDirPath;
    }

    public String getWorkersDirPath() {
        return workersDirPath;
    }

    @Override
    public void setInternalURI(MultiURI u) {
        for (CommAdaptor adaptor : Comm.getAdaptors().values()) {
            adaptor.completeMasterURI(u);
        }
    }

    @Override
    public Type getType() {
        return Type.MASTER;
    }

    @Override
    public int compareTo(Resource t) {
        if (t.getType() == Type.MASTER) {
            return getName().compareTo(t.getName());
        } else {
            return 1;
        }
    }

    public void updateResource(Map<String, String> sharedDisks) {
        super.sharedDisks = sharedDisks;
    }
     */
    public void updateResource(MethodResourceDescription rd, Map<String, String> sharedDisks);

}
