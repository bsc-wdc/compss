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
import es.bsc.compss.monitoringparsers.RuntimeLogParser;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class ExecutionInformationViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_EXEC_INFO);

    private String displayType;


    @Init
    public void init() {
        this.displayType = new String(Constants.EIL_TASKS_WITH_FAILED_JOBS);
    }

    /**
     * Returns the registered tasks.
     * 
     * @return The registered tasks.
     */
    public List<ExecutionInformationTask> getTasks() {
        if (this.displayType.equals(Constants.EIL_ALL)) {
            // First entry is always empty. TaskId >= 1, JobId >= 1
            if (RuntimeLogParser.getTasks().size() > 1) {
                return new ListModelList<ExecutionInformationTask>(
                    RuntimeLogParser.getTasks().subList(1, RuntimeLogParser.getTasks().size()));
            } else {
                return new ListModelList<ExecutionInformationTask>();
            }
        } else if (this.displayType.equals(Constants.EIL_CURRENT_TASKS)) {
            return new ListModelList<ExecutionInformationTask>(RuntimeLogParser.getTasksCurrent());
        } else if (this.displayType.equals(Constants.EIL_FAILED_TASKS)) {
            return new ListModelList<ExecutionInformationTask>(RuntimeLogParser.getTasksFailed());
        } else if (this.displayType.equals(Constants.EIL_TASKS_WITH_FAILED_JOBS)) {
            return new ListModelList<ExecutionInformationTask>(RuntimeLogParser.getTasksWithFailedJobs());
        }
        return new ListModelList<ExecutionInformationTask>();
    }

    public String getDisplayType() {
        return this.displayType;
    }

    public void setDisplayType(String displayType) {
        this.displayType = displayType;
    }

    /**
     * Updates the execution information view model.
     */
    @Command
    @NotifyChange("tasks")
    public void update() {
        LOGGER.debug("Updating Execution Information ViewModel...");
        RuntimeLogParser.parse();
        LOGGER.debug("Execution Information ViewModel updated");
    }

    @Command
    @NotifyChange("tasks")
    public void clear() {
    }

}
