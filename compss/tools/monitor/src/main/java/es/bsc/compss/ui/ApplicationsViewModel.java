package es.bsc.compss.ui;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zk.ui.Sessions;
import org.zkoss.zul.ListModelList;

import es.bsc.compss.commons.Loggers;
import es.bsc.compss.ui.auth.UserCredential;


public class ApplicationsViewModel {

    private List<Application> applications;
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_APPLICATIONS);


    @Init
    public void init() {
        applications = new LinkedList<>();
        update();
    }

    public List<Application> getApplications() {
        return new ListModelList<Application>(this.applications);
    }

    @Command
    @NotifyChange("applications")
    public void update() {
        logger.debug("Updating Applications ViewModel...");
        // Erase all current applications
        applications.clear();
        setSelectedApp("");

        // Import new resources
        String appsLocation = ((UserCredential) Sessions.getCurrent().getAttribute("userCredential")).getCOMPSs_BASE_LOG();
        File COMPSs_LOG_DIR = new File(appsLocation);
        if (COMPSs_LOG_DIR.exists()) {
            for (File f : COMPSs_LOG_DIR.listFiles()) {
                logger.debug("Adding application " + f.getName());
                Application app = new Application(f.getName(), appsLocation + File.separator + f.getName());
                applications.add(app);
            }
        }

        if (Properties.isSortApplications()) {
            Collections.sort(applications, new ApplicationComparator());
        }

        logger.debug("Applications ViewModel updated");
    }

    @Command
    public void setSelectedApp(@BindingParam("appName") String appName) {
        logger.debug("Updating Selected Application...");
        Application selectedApp = new Application();
        for (Application app : applications) {
            if (app.getName().equals(appName)) {
                selectedApp = new Application(app);
                break;
            }
        }
        // Set global variables to selected app
        Properties.setBasePath(selectedApp.getPath());
        ((UserCredential) Sessions.getCurrent().getAttribute("userCredential")).setMonitoredApp(selectedApp);
        logger.debug("Selected application updated");
    }


    private class ApplicationComparator implements Comparator<Application> {

        @Override
        public int compare(Application app1, Application app2) {
            return app1.getName().compareTo(app2.getName());
        }

    }

}
