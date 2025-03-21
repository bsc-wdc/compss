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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class StatisticsViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_STATISTICS);

    private List<StatisticParameter> statistics;


    @Init
    public void init() {
        this.statistics = new LinkedList<>();
    }

    public List<StatisticParameter> getStatistics() {
        return new ListModelList<StatisticParameter>(this.statistics);
    }

    /**
     * Updates the statistics view model.
     * 
     * @param statisticsParameters New statistic paramters with their values.
     */
    @Command
    @NotifyChange("statistics")
    public void update(HashMap<String, String> statisticsParameters) {
        LOGGER.debug("Updating Statistics ViewModel...");
        // Erase all current resources
        this.statistics.clear();

        // Import new values
        for (Entry<String, String> entry : statisticsParameters.entrySet()) {
            this.statistics.add(new StatisticParameter(entry.getKey(), entry.getValue()));
        }

        LOGGER.debug("Statistics ViewModel updated");
    }

    @Command
    @NotifyChange("statistics")
    public void clear() {
        // Erase all current resources
        this.statistics.clear();
    }

}
