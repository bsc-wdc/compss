package es.bsc.compss.types;

import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.log.Loggers;

import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class WallClockTimerTask extends TimerTask {

    private final Application app;
    private final AccessProcessor ap;
    private final COMPSsRuntime rt;

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
    public WallClockTimerTask(Application app, AccessProcessor ap, COMPSsRuntime rt) {
        this.app = app;
        this.ap = ap;
        this.rt = rt;
    }

    @Override
    public void run() {
        LOGGER.warn("WARNING: Wall clock limit reached for app " + app.getId() + "! Cancelling tasks...");
        ap.cancelApplicationTasks(app);
        if (rt != null) {
            ap.noMoreTasks(app);
            ap.getResultFiles(app);
            rt.stopIT(true);
            System.exit(0);
        }
    }

}
