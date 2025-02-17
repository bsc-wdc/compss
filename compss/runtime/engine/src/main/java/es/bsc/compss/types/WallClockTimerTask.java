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
package es.bsc.compss.types;

import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.log.Loggers;

import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WallClockTimerTask extends TimerTask {

    private final Application app;
    private final AccessProcessor ap;
    private final COMPSsRuntimeImpl rt;

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Creates a timer task to execute when the application wall clock limit has been exceeded.
     * 
     * @param app Application to set the wall clock limit.
     * @param ap Access Processor reference to cancel submitted applications.
     * @param rt Runtime reference to stop and exit the application. (Null if this step must be skipped)
     */
    public WallClockTimerTask(Application app, AccessProcessor ap, COMPSsRuntimeImpl rt) {
        this.app = app;
        this.ap = ap;
        this.rt = rt;
    }

    @Override
    public void run() {
        LOGGER.warn("WARNING: Wall clock limit reached for app " + app.getId() + "! Cancelling tasks...");
        ap.cancelApplicationTasks(app);
        if (rt != null) {
            rt.noMoreTasks(app);
            rt.stopIT(true);
            System.exit(0);
        }
    }

}
