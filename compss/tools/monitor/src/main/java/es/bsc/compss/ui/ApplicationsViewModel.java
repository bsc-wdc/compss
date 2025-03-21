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
import es.bsc.compss.ui.auth.UserCredential;

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


public class ApplicationsViewModel {

    private List<Application> applications;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_APPLICATIONS);


    @Init
    public void init() {
        this.applications = new LinkedList<>();
        update();
    }

    public List<Application> getApplications() {
        return new ListModelList<Application>(this.applications);
    }

    /**
     * Update applications view model.
     */
    @Command
    @NotifyChange("applications")
    public void update() {
        LOGGER.debug("Updating Applications ViewModel...");
        // Erase all current applications
        this.applications.clear();
        setSelectedApp("");

        // Import new resources
        String appsLocation =
            ((UserCredential) Sessions.getCurrent().getAttribute("userCredential")).getCompssBaseLog();
        File compssLogDir = new File(appsLocation);
        if (compssLogDir.exists()) {
            for (File f : compssLogDir.listFiles()) {
                LOGGER.debug("Adding application " + f.getName());
                Application app = new Application(f.getName(), appsLocation + File.separator + f.getName());
                this.applications.add(app);
            }
        }

        if (Properties.isSortApplications()) {
            Collections.sort(this.applications, new ApplicationComparator());
        }

        LOGGER.debug("Applications ViewModel updated");
    }

    /**
     * Updates the view model with the selected application.
     * 
     * @param appName Application name loaded from UI
     */
    @Command
    public void setSelectedApp(@BindingParam("appName") String appName) {
        LOGGER.debug("Updating Selected Application...");
        Application selectedApp = new Application();
        for (Application app : this.applications) {
            if (app.getName().equals(appName)) {
                selectedApp = new Application(app);
                break;
            }
        }
        // Set global variables to selected app
        Properties.setBasePath(selectedApp.getPath());
        ((UserCredential) Sessions.getCurrent().getAttribute("userCredential")).setMonitoredApp(selectedApp);
        LOGGER.debug("Selected application updated");
    }


    private class ApplicationComparator implements Comparator<Application> {

        @Override
        public int compare(Application app1, Application app2) {
            return app1.getName().compareTo(app2.getName());
        }

    }

}
