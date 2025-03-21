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

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class CoresViewModel {

    private List<Core> cores;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_TASKS);


    @Init
    public void init() {
        this.cores = new LinkedList<>();
    }

    public List<Core> getCores() {
        return new ListModelList<Core>(this.cores);
    }

    /**
     * Updates the cores view model.
     * 
     * @param newCoreData New parsed data from the monitoring parsers.
     */
    @Command
    @NotifyChange("cores")
    public void update(List<String[]> newCoreData) {
        LOGGER.debug("Updating Tasks ViewModel...");
        // Erase all current resources
        this.cores.clear();

        // Import new resources
        for (String[] dc : newCoreData) {
            /*
             * Each entry on the newCoreData array is of the form: coreId, implId, signature, meanET, minET, maxET,
             * execCount
             */
            if (LOGGER.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder();
                sb.append("   - CoreElement: ");
                for (String elem : dc) {
                    sb.append(elem).append(" ");
                }
                LOGGER.debug(sb.toString());
            }

            // Check color
            int taskId = Integer.parseInt(dc[0]) + 1; // +1 To shift according to COLORS and tracing
            int colorId = taskId % Constants.CORE_COLOR_MAX;
            String color = File.separator + "images" + File.separator + "colors" + File.separator + colorId + ".png";

            // color, dc
            Core c = new Core(color, dc);
            this.cores.add(c);
        }
        LOGGER.debug("Tasks ViewModel updated");
    }

    @Command
    @NotifyChange("cores")
    public void clear() {
        this.cores.clear();
    }

}
