/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.gos.master.monitoring.jobmonitor;

import es.bsc.compss.gos.master.GOSJob;

import java.util.concurrent.ConcurrentHashMap;


public class GOSHostsInteractiveManager implements GOSHostsManager {

    protected final ConcurrentHashMap<String, GOSSingleJobManager> activeJobs = new ConcurrentHashMap<>();


    /**
     * Instantiates a new Gos hosts manager.
     *
     * @param job the job
     */
    public GOSHostsInteractiveManager(GOSJob job) {
        addJobMonitor(job);
    }

    @Override
    public synchronized void addJobMonitor(GOSJob job) {
        activeJobs.put(job.getCompositeID(), new GOSSingleJobManager(job));
    }

    @Override
    public boolean existsRunningJobs() {
        return !activeJobs.isEmpty();
    }

    /**
     * Monitor.
     */
    public void monitor() {
        for (Object o : activeJobs.values().toArray()) {
            GOSSingleJobManager sjm = (GOSSingleJobManager) o;
            if (sjm.monitor()) {
                removeJobMonitor(sjm.getID());
            }
        }
    }

    private synchronized void removeJobMonitor(String id) {
        activeJobs.remove(id);
    }

    public int countActiveJobs() {
        return activeJobs.size();
    }

    @Override
    public void shutdown() {
        for (GOSSingleJobManager jm : activeJobs.values()) {
            jm.shutdown();
        }
        activeJobs.clear();

    }

}
