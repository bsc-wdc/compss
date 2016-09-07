package integratedtoolkit.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.api.COMPSsRuntime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Manages warnings, errors and fatal errors. Stops the IT and does a System.exit(1) in errors and fatal errors cases.
 * 
 */
public final class ErrorManager {

    public static final String NEWLINE = "\r\n";
    private static final String REGEX_NEWLINE = NEWLINE + "|\n|\r";

    private static final String PREFIX_ERRMGR = "[ERRMGR]  -  ";
    private static final String PREFIX_WARNING = PREFIX_ERRMGR + "WARNING: ";
    private static final String PREFIX_ERROR = PREFIX_ERRMGR + "ERROR:   ";
    private static final String PREFIX_FATAL = PREFIX_ERRMGR + "FATAL:   ";
    private static final String SUFFIX_SHUTTING_DOWN = PREFIX_ERRMGR + "Shutting down COMPSs...";

    private static final Integer REQUEST_ERROR = 1;
    private static final Integer REQUEST_FATAL = 2;

    private static final Logger logger = LogManager.getLogger(Loggers.ERROR_MANAGER);

    private static COMPSsRuntime compssRuntime = null;
    private static Integer errorRequest = -1;

    private static boolean stopping = false;

    /**
     * It handles ERROR and FATAL messages asynchronously
     */
    private static Runnable errorRunnable = new Runnable() {

        public void run() {
            if (errorRequest == REQUEST_ERROR || errorRequest == REQUEST_FATAL) {
                if (compssRuntime != null) {
                    logger.error(PREFIX_ERRMGR + "Error detected. Shutting down COMPSs");
                    compssRuntime.stopIT(true);
                }

                System.exit(1);
            }
        }
    };


    /**
     * Initializes the ErrorManager
     * 
     * @param compssRuntime
     */
    public static void init(COMPSsRuntime compssRuntime) {
        ErrorManager.compssRuntime = compssRuntime;
    }

    /**
     * Warning handling (just print it)
     * 
     * @param msg
     * @param e
     */
    public static void warn(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_WARNING, msg, e, System.out);
        }

        if (logger != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                logger.warn(line);
            }
        }
    }

    /**
     * Warning handling (just print it)
     * 
     * @param e
     */
    public static void warn(Exception e) {
        warn("", e);
    }

    /**
     * Warning handling (just print it)
     * 
     * @param msg
     */
    public static void warn(String msg) {
        warn(msg, null);
    }

    /**
     * Error handling
     * 
     * @param msg
     * @param e
     */
    public static synchronized void error(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_ERROR, msg, e, System.err);
            prettyPrint("", SUFFIX_SHUTTING_DOWN, null, System.err);

            stopping = true;
            errorRequest = REQUEST_ERROR;
            new Thread(errorRunnable, "ErrorManager Error Thread").start();
        }

        if (logger != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                logger.error(line);
            }
        }
    }

    /**
     * Error handling
     * 
     * @param e
     */
    public static void error(Exception e) {
        error("", e);
    }

    /**
     * Error handling
     * 
     * @param msg
     */
    public static void error(String msg) {
        error(msg, null);
    }

    /**
     * Fatal handling
     * 
     * @param msg
     * @param e
     */
    public static synchronized void fatal(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_FATAL, msg, e, System.err);
            prettyPrint("", SUFFIX_SHUTTING_DOWN, null, System.err);

            stopping = true;
            errorRequest = REQUEST_FATAL;
            new Thread(errorRunnable, "ErrorManager Fatal Thread").start();
        }

        if (logger != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                logger.fatal(line);
            }
        }
    }

    /**
     * Fatal handling
     * 
     * @param e
     */
    public static void fatal(Exception e) {
        fatal("", e);
    }

    /**
     * Fatal handling
     * 
     * @param msg
     */
    public static void fatal(String msg) {
        fatal(msg, null);
    }

    /*
     * **************************************************** 
     * PRIVATE METHODS
     ****************************************************/

    /**
     * Indents every line so that a single warning, error or fatal shows as a unique block, including exceptions and
     * stack trace
     * 
     * @param prefix
     * @param _msg
     * @param e
     * @param ps
     */
    private static void prettyPrint(String prefix, String _msg, Exception e, PrintStream ps) {
        // Append exception message and stackTrace
        String msg = _msg;

        if (e != null) {
            msg += NEWLINE;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            msg += "Stack trace:" + NEWLINE + sw.toString();
        }

        String[] lines = msg.split(REGEX_NEWLINE);
        for (int i = 0; i < lines.length; ++i) {
            String l = lines[i];
            if (i == 0) { // Add prefix
                l = prefix + l;
            } else {
                l = indent(l, prefix.length());
            }
            ps.println(l);
        }
    }

    /**
     * Adds indentation to a string
     * 
     * @param str
     * @param indentation
     * @return
     */
    private static String indent(String str, int indentation) {
        for (int j = 0; j < indentation; ++j) {
            str = " " + str;
        }
        return str;
    }

    /**
     * Private constructor to avoid instantiation
     * 
     */
    private ErrorManager() {
        // No possible instantiation of this class
        throw new UnsupportedOperationException();
    }

}
