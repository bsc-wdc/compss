package com.bsc.compss.ui;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.GlobalCommand;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zk.ui.Session;
import org.zkoss.zk.ui.Sessions;
import org.zkoss.zk.ui.event.Event;
import org.zkoss.zk.ui.event.EventListener;
import org.zkoss.zul.Messagebox;

import com.bsc.compss.commons.Loggers;
import com.bsc.compss.ui.auth.UserCredential;

import monitoringParsers.*;


public class ViewModel {

    private ResourcesViewModel resourcesViewModel;
    private CoresViewModel coresViewModel;
    private CurrentGraphViewModel currentGraphViewModel;
    private CompleteGraphViewModel completeGraphViewModel;
    private LoadChartViewModel loadChartViewModel;
    private RuntimeLogViewModel runtimeLogViewModel;
    private ExecutionInformationViewModel executionInformationViewModel;
    private StatisticsViewModel statisticsViewModel;

    private String selectedTab;

    private static int runtimeLogConfirmation = -1; // -1: do not apply, 0: accepted, 1:denied

    private static final Logger logger = LogManager.getLogger(Loggers.UI_VMS);


    @Init
    public void init() {
        logger.debug("Initializing Resources ViewModel Structure...");
        resourcesViewModel = new ResourcesViewModel();
        resourcesViewModel.init();

        logger.debug("Initializing Tasks ViewModel Structure...");
        coresViewModel = new CoresViewModel();
        coresViewModel.init();

        logger.debug("Initializing Current Graph ViewModel Structure...");
        currentGraphViewModel = new CurrentGraphViewModel();
        currentGraphViewModel.init();

        logger.debug("Initializing Complete Graph ViewModel Structure...");
        completeGraphViewModel = new CompleteGraphViewModel();
        completeGraphViewModel.init();

        logger.debug("Initializing Resources Load Chart ViewModel Structure...");
        loadChartViewModel = new LoadChartViewModel();
        loadChartViewModel.init();

        logger.debug("Initializing it.log Structure...");
        runtimeLogViewModel = new RuntimeLogViewModel();
        runtimeLogViewModel.init();

        logger.debug("Initializing Execution Information Structure...");
        executionInformationViewModel = new ExecutionInformationViewModel();
        executionInformationViewModel.init();

        logger.debug("Initializing Statistics Structure...");
        statisticsViewModel = new StatisticsViewModel();
        statisticsViewModel.init();

        logger.debug("Initalizing private structures...");
        selectedTab = new String(Constants.resourcesInformationTabName);

        logger.info("Initialization DONE");

        // Update information
        update();
    }

    public ResourcesViewModel getResourcesViewModel() {
        return this.resourcesViewModel;
    }

    public CoresViewModel getCoresViewModel() {
        return this.coresViewModel;
    }

    public CurrentGraphViewModel getCurrentGraphViewModel() {
        return this.currentGraphViewModel;
    }

    public CompleteGraphViewModel getCompleteGraphViewModel() {
        return this.completeGraphViewModel;
    }

    public LoadChartViewModel getLoadChartViewModel() {
        return this.loadChartViewModel;
    }

    public RuntimeLogViewModel getRuntimeLogViewModel() {
        return this.runtimeLogViewModel;
    }

    public ExecutionInformationViewModel getExecutionInformationViewModel() {
        return this.executionInformationViewModel;
    }

    public StatisticsViewModel getStatisticsViewModel() {
        return this.statisticsViewModel;
    }

    public int getRefreshTime() {
        return Properties.getRefreshTime();
    }

    @Command
    @NotifyChange({ "resourcesViewModel", "coresViewModel", "currentGraphViewModel", "completeGraphViewModel", "loadChartViewModel",
            "runtimeLogViewModel", "executionInformationViewModel", "statisticsViewModel" })
    public void select(@BindingParam("selectedTab") String selectedTab) {
        if (!this.selectedTab.equals(selectedTab)) {
            this.selectedTab = selectedTab;

            // Runtime log message box protection
            if (selectedTab.equals(Constants.runtimeLogTabName)) {
                logger.debug("Trying to load runtime.log, displaying messagebox");
                runtimeLogConfirmation = -1;
                Messagebox.show(
                        "The runtime.log can be huge and you may experience slowness loading this tab. Do you really want to load it?",
                        "Warning", Messagebox.OK | Messagebox.CANCEL, Messagebox.QUESTION, new EventListener<Event>() {

                            public void onEvent(Event e) {
                                if (Messagebox.ON_OK.equals(e.getName())) {
                                    runtimeLogConfirmation = 0;
                                } else {
                                    runtimeLogConfirmation = 1;
                                }
                            }
                        });
            } else {
                // Normal tab selection
                this.update();
                this.updateRuntimeLog();
                this.updateExecutionInformation();
            }
        }
    }

    @Command
    @NotifyChange({ "resourcesViewModel", "coresViewModel", "currentGraphViewModel", "completeGraphViewModel", "loadChartViewModel",
            "statisticsViewModel", "runtimeLogViewModel" })
    public void update() {
        logger.debug("Loading Monitored Application...");
        Application monitoredApp = new Application();
        Session session = Sessions.getCurrent();
        if (session != null) {
            UserCredential userCred = ((UserCredential) session.getAttribute("userCredential"));
            if (userCred != null) {
                monitoredApp = userCred.getMonitoredApp();
            }
        }
        logger.info("Loaded Monitored Application: " + monitoredApp.getName());

        if (monitoredApp.getName() != "") {
            if (selectedTab.equals(Constants.resourcesInformationTabName)) {
                logger.debug("Updating Resources Information...");
                // Monitor XML parse
                logger.debug("Parsing Monitor XML File...");
                MonitorXmlParser.parseResources();
                logger.debug("Monitor XML File parsed");
                // Update
                resourcesViewModel.update(MonitorXmlParser.getWorkersDataArray());
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.tasksInformationTabName)) {
                logger.debug("Updating Jobs Information...");
                // Monitoring parse
                logger.debug("Parsing Monitor XML File...");
                MonitorXmlParser.parseCores();
                logger.debug("Monitor XML File parsed");
                // Update
                coresViewModel.update(MonitorXmlParser.getCoresDataArray());
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.currentTasksGraphTabName)) {
                logger.debug("Updating Current Tasks Graph...");
                // Monitor XML parse
                logger.debug("Parsing Monitor XML File...");
                MonitorXmlParser.parseCores();
                logger.debug("Monitor XML File parsed");
                // Update
                coresViewModel.update(MonitorXmlParser.getCoresDataArray());
                currentGraphViewModel.update(monitoredApp);
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.completeTasksGraphTabName)) {
                logger.debug("Updating Complete Tasks Graph...");
                // Monitor XML parse
                logger.debug("Parsing Monitor XML File...");
                MonitorXmlParser.parseCores();
                logger.debug("Monitor XML File parsed");
                // Update
                coresViewModel.update(MonitorXmlParser.getCoresDataArray());
                completeGraphViewModel.update(monitoredApp);
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.loadChartTabName)) {
                logger.debug("Updating Resouces Load Chart...");
                // Update
                loadChartViewModel.update();
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.statisticsTabName)) {
                logger.debug("Updating statistics...");
                // Monitor XML parse
                logger.debug("Parsing Monitor XML File...");
                MonitorXmlParser.parseStatistics();
                logger.debug("Monitor XML File parsed");
                // Update
                statisticsViewModel.update(MonitorXmlParser.getStatisticsParameters());
                logger.info("Structures updated");
            } else if (selectedTab.equals(Constants.runtimeLogTabName)) {
                // Wait for messagebox handler. If already answered do nothing because this tab has no automatic update
                // Check messagebox result
                if (runtimeLogConfirmation == 0) {
                    logger.debug("Messagebox confirmation received. Loading runtime.log");
                    this.updateRuntimeLog();
                    // Reset messagebox handler to avoid automatic refresh
                    runtimeLogConfirmation = -1;
                } else if (runtimeLogConfirmation == 1) {
                    logger.debug("Messagebox denied");
                    // Reset messagebox handler to avoid automatic refresh
                    runtimeLogConfirmation = -1;
                }
            } else if (selectedTab.equals(Constants.executionInformationTabName)) {
                // Nothing to do. This tab doesn't have automatic update
            } else {
                logger.info("No Information Tab selected");
            }
        } else {
            resourcesViewModel.clear();
            coresViewModel.clear();
            currentGraphViewModel.clear();
            completeGraphViewModel.clear();
            loadChartViewModel.clear();
            statisticsViewModel.clear();
            runtimeLogViewModel.clear();
            logger.info("No Application Selected");
        }
    }

    @Command
    @NotifyChange("runtimeLogViewModel")
    public void updateRuntimeLog() {
        logger.debug("Loading Monitored Application...");
        Application monitoredApp = new Application();
        Session session = Sessions.getCurrent();
        if (session != null) {
            UserCredential userCred = ((UserCredential) session.getAttribute("userCredential"));
            if (userCred != null) {
                monitoredApp = userCred.getMonitoredApp();
            }
        }
        logger.debug("Loaded Monitored Application: " + monitoredApp.getName());

        logger.debug("Updating RuntimeLog...");
        if (monitoredApp.getName() != "") {
            if (selectedTab.equals(Constants.runtimeLogTabName)) {
                runtimeLogViewModel.update();
            } else {
                runtimeLogViewModel.clear();
            }
        } else {
            runtimeLogViewModel.clear();
        }

        logger.info("Runtime.log updated");
    }

    @Command
    @NotifyChange("executionInformationViewModel")
    public void updateExecutionInformation() {
        logger.debug("Loading Monitored Application...");
        Application monitoredApp = new Application();
        Session session = Sessions.getCurrent();
        if (session != null) {
            UserCredential userCred = ((UserCredential) session.getAttribute("userCredential"));
            if (userCred != null) {
                monitoredApp = userCred.getMonitoredApp();
            }
        }
        logger.debug("Loaded Monitored Application: " + monitoredApp.getName());

        logger.debug("Updating Execution Information...");
        if (monitoredApp.getName() != "") {
            if (selectedTab.equals(Constants.executionInformationTabName)) {
                executionInformationViewModel.update();
            }
        } else {
            executionInformationViewModel.clear();
        }

        logger.info("Execution Information updated");
    }

    @Command
    @NotifyChange("loadChartViewModel")
    public void setDivUUID(@BindingParam("divuuid") String divuuid) {
        loadChartViewModel.setDivUUID(divuuid);
    }

    @Command
    public void downloadCompleteGraph() {
        completeGraphViewModel.download();
    }

    @Command
    public void downloadCurrentGraph() {
        currentGraphViewModel.download();
    }

    @GlobalCommand
    @NotifyChange({ "resourcesViewModel", "coresViewModel", "currentGraphViewModel", "completeGraphViewModel", "loadChartViewModel",
            "runtimeLogViewModel", "executionInformationViewModel", "statisticsViewModel" })
    public void refresh() {
        this.update();
        this.updateRuntimeLog();
        this.updateExecutionInformation();
    }

}
