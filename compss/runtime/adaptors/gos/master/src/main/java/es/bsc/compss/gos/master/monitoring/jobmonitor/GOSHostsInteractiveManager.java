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
package es.bsc.compss.gos.master.monitoring.jobmonitor;

import es.bsc.compss.gos.master.GOSJob;

import java.util.Collection;
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
    public void addJobMonitor(GOSJob job) {
        synchronized (activeJobs) {
            activeJobs.put(job.getCompositeID(), new GOSSingleJobManager(job));
        }

    }

    @Override
    public boolean existsRunningJobs() {
        return !activeJobs.isEmpty();
    }

    /**
     * Monitor.
     */
    public void monitor() {
        if (activeJobs.isEmpty()) {
            return;
        }

        GOSSingleJobManager[] jobs = null;
        synchronized (activeJobs) {
            Collection<GOSSingleJobManager> col = activeJobs.values();
            if (!col.isEmpty()) {
                jobs = col.toArray(new GOSSingleJobManager[col.size()]);
            }
        }
        if (jobs == null) {
            return;
        }

        for (GOSSingleJobManager sjm : jobs) {
            if (sjm.monitor()) {
                removeJobMonitorID(sjm.getID());
            }
        }
    }

    private void removeJobMonitorID(String id) {
        synchronized (activeJobs) {
            activeJobs.remove(id);
        }
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

    @Override
    public void removeJobMonitor(GOSJob job) {
        removeJobMonitorID(job.getCompositeID());

    }
}
