package es.bsc.compss.agent;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.AgentTraceMerger;

import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;


public class TraceManager {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();


    private static void loggerSetUp() {
        System.setProperty(COMPSsConstants.APP_LOG_DIR, "traceMergeLog");
        ((LoggerContext) LogManager.getContext(false)).reconfigure();
    }

    /**
     * Merges the traces generated traceDirs directories and outputs the resulting trace in outputDir. "appName"
     * "outputDir" traceDirs
     */
    public static void main(String[] args) {
        loggerSetUp();
        String appName = args[0];
        String outputDir = args[1];
        String[] traceDirs = Arrays.copyOfRange(args, 2, args.length);
        System.out.println("______appName = " + appName);
        System.out.println("______outputDir = " + outputDir);
        System.out.println("______args.lenght = " + args.length);
        System.out.println("______traceDirs.lenght = " + traceDirs.length);
        System.out.println("______traceDirs[0] = " + traceDirs[0]);
        System.out.println("______traceDirs[1] = " + traceDirs[1]);
        try {
            AgentTraceMerger tm = new AgentTraceMerger(outputDir, traceDirs, appName);
            System.out.println("______llamando al merger desde TraceManager");
            tm.merge();
        } catch (Throwable t) {
            System.out.println("______exeption" + t.toString());
            t.printStackTrace();
        }
    }
}