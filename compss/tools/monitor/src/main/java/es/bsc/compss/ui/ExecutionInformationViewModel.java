package es.bsc.compss.ui;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;

import es.bsc.compss.commons.Loggers;

import monitoringParsers.RuntimeLogParser;


public class ExecutionInformationViewModel {

    private String displayType;
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_EXEC_INFO);


    @Init
    public void init() {
        displayType = new String(Constants.EIL_TASKS_WITH_FAILED_JOBS);
    }

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

    @Command
    @NotifyChange("tasks")
    public void update() {
        logger.debug("Updating Execution Information ViewModel...");
        RuntimeLogParser.parse();
        logger.debug("Execution Information ViewModel updated");
    }

    @Command
    @NotifyChange("tasks")
    public void clear() {
    }

}
