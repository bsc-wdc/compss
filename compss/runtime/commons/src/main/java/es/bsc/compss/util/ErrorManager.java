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
package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.ErrorHandler;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Manages warnings, errors and fatal errors. Stops the COMPSs Runtime and does a System.exit(1) in errors and fatal
 * errors cases.
 */
public final class ErrorManager {

    public static final String NEWLINE = "\r\n";
    private static final String REGEX_NEWLINE = NEWLINE + "|\n|\r";

    private static final String PREFIX_ERRMGR = "[ERRMGR]  -  ";
    private static final String PREFIX_WARNING = PREFIX_ERRMGR + "WARNING: ";
    private static final String PREFIX_ERROR = PREFIX_ERRMGR + "ERROR:   ";
    private static final String PREFIX_FATAL = PREFIX_ERRMGR + "FATAL:   ";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.ERROR_MANAGER);

    private static ErrorHandler handler = null;

    private static boolean stopping = false;


    /**
     * Initializes the ErrorManager.
     *
     * @param handler Class to invoke on a fatal error case.
     */
    public static void init(ErrorHandler handler) {
        ErrorManager.handler = handler;
    }

    /**
     * Handles a warning message and/or exception (prints it).
     *
     * @param msg Warning message.
     * @param e Warning exception.
     */
    public static void warn(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_WARNING, msg, e, System.err);
        }

        if (LOGGER != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                LOGGER.warn(line);
            }
        }
    }

    /**
     * Handles a warning exception (prints it).
     *
     * @param e Warning exception.
     */
    public static void warn(Exception e) {
        warn("", e);
    }

    /**
     * Handles a warning message (prints it).
     *
     * @param msg Warning message.
     */
    public static void warn(String msg) {
        warn(msg, null);
    }

    /**
     * Handles an error message and/or exception (prints it and stops the Runtime).
     *
     * @param msg Error message.
     * @param e Error exception.
     */
    public static synchronized void error(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_ERROR, msg, e, System.err);
            stopping = handler.handleError();
        }

        if (LOGGER != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                LOGGER.error(line);
            }
        }
    }

    /**
     * Handles an error exception (prints it and stops the Runtime).
     *
     * @param e Error exception.
     */
    public static void error(Exception e) {
        error("", e);
    }

    /**
     * Handles an error message (prints it and stops the Runtime).
     *
     * @param msg Error message.
     */
    public static void error(String msg) {
        error(msg, null);
    }

    /**
     * Handles an fatal message and/or exception (prints it and stops the Runtime).
     *
     * @param msg Fatal message.
     * @param e Fatal exception.
     */
    public static synchronized void fatal(String msg, Exception e) {
        if (!stopping) {
            prettyPrint(PREFIX_FATAL, msg, e, System.err);

            stopping = handler.handleFatalError();
        }

        if (LOGGER != null) {
            for (String line : msg.split(REGEX_NEWLINE)) {
                LOGGER.fatal(line);
            }
        }
    }

    /**
     * Handles an fatal exception (prints it and stops the Runtime).
     *
     * @param e Fatal exception.
     */
    public static void fatal(Exception e) {
        fatal("", e);
    }

    /**
     * Handles an fatal message (prints it and stops the Runtime).
     *
     * @param msg Fatal message.
     */
    public static void fatal(String msg) {
        fatal(msg, null);
    }

    /**
     * Prints an information message through the ErrorManager.
     *
     * @param message Message to print
     * @param e Exception causing the message
     * @param channel Stream where to print the message
     */
    public static void info(String message, Exception e, PrintStream channel) {
        prettyPrint(PREFIX_ERRMGR, message, e, channel);
    }

    /**
     * Prints an information message through the ErrorManager on through its logger.
     *
     * @param message Message to print
     * @param e Exception causing the message
     */
    public static void logInfo(String message, Exception e) {
        LOGGER.info(PREFIX_ERRMGR + message, e);
    }

    /**
     * Prints an information message through the ErrorManager on through its logger.
     *
     * @param message Message to print
     * @param e Exception causing the message
     */
    public static void logError(String message, Exception e) {
        LOGGER.error(PREFIX_ERRMGR + message, e);
    }

    /*
     * *************************************************************************************************************
     * PRIVATE METHODS
     **************************************************************************************************************/
    /**
     * Indents every line so that a single warning, error or fatal shows as a unique block, including exceptions and
     * stack trace.
     *
     * @param prefix Print prefix.
     * @param msg Print message.
     * @param e Print exception.
     * @param ps Output PrintStream.
     */
    private static void prettyPrint(String prefix, String msg, Exception e, PrintStream ps) {
        // Append exception message and stackTrace
        String prettyMsg = msg;

        if (e != null) {
            prettyMsg += NEWLINE;
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            prettyMsg += "Stack trace:" + NEWLINE + sw.toString();
        }

        String[] lines = prettyMsg.split(REGEX_NEWLINE);
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
     * Adds indentation to a string.
     *
     * @param str Input string.
     * @param indentation Indentation value.
     * @return The given string {@code str} prepended with {@code indentation} indentation spaces.
     */
    private static String indent(String str, int indentation) {
        for (int j = 0; j < indentation; ++j) {
            str = " " + str;
        }
        return str;
    }

    /**
     * Private constructor to avoid instantiation.
     */
    private ErrorManager() {
        // No possible instantiation of this class
        throw new UnsupportedOperationException();
    }

}
