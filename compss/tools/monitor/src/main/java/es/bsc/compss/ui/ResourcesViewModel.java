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
package es.bsc.compss.ui;

import es.bsc.compss.commons.Loggers;

import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class ResourcesViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_RESOURCES);

    private List<Resource> resources;


    @Init
    public void init() {
        this.resources = new LinkedList<>();
    }

    public List<Resource> getResources() {
        return new ListModelList<Resource>(this.resources);
    }

    /**
     * Updates the resources view model with the new parsed data {@code newResourcesData}.
     * 
     * @param newResourcesData New data parsed by the monitoring parsers.
     */
    @Command
    @NotifyChange("resources")
    public void update(List<String[]> newResourcesData) {
        LOGGER.debug("Updating Resources ViewModel...");
        // Erase all current resources
        this.resources.clear();

        // Import new resources
        for (String[] dr : newResourcesData) {
            /*
             * Each entry in the new Resource data is of the form: workerName totalCPUu totalGPUu totalFPGAu totalOTHERu
             * memory disk status provider image actions
             */

            // Change format of some fields

            // Check memSize
            if (dr[5] != null) {
                if (dr[5].startsWith("0.")) {
                    Float memsize = Float.parseFloat(dr[5]);
                    dr[5] = String.valueOf(memsize * 1024) + " MB";
                } else if (!dr[5].isEmpty()) {
                    dr[5] = dr[5] + " GB";
                } else {
                    dr[5] = "-";
                }
            } else {
                dr[5] = "-";
            }

            // Check Disk Size
            if (dr[6] != null) {
                if (dr[6].startsWith("0.")) {
                    Float disksize = Float.parseFloat(dr[6]);
                    dr[6] = String.valueOf(disksize * 1024) + " MB";
                } else if (!dr[6].isEmpty()) {
                    dr[6] = dr[6] + " GB";
                } else {
                    dr[6] = "-";
                }
            } else {
                dr[6] = "-";
            }

            Resource r = new Resource(dr);
            this.resources.add(r);
        }
        LOGGER.debug("Resources ViewModel updated");
    }

    @Command
    @NotifyChange("resources")
    public void clear() {
        this.resources.clear();
    }

}
