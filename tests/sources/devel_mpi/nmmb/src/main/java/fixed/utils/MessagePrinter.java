package fixed.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import nmmb.loggers.LoggerNames;


public class MessagePrinter {

    // Loggers
    private static final Logger LOGGER_FIXED = LogManager.getLogger(LoggerNames.NMMB_FIXED);

    // For info messages
    private static final int LINE_SIZE = 60;
    private static final int PRE_CHARS_HEADER_LINE = 11;
    private static final int PRE_CHARS_MSG_LINE = 5;


    /**
     * Prints @msg as header message
     * 
     * @param msg
     */
    public static void printHeaderMsg(String msg) {
        // Separator line
        LOGGER_FIXED.info("");

        // Message line
        StringBuilder sb = new StringBuilder("========= ");
        sb.append(msg);
        sb.append(" ");
        for (int i = PRE_CHARS_HEADER_LINE + msg.length(); i < LINE_SIZE; ++i) {
            sb.append("=");
        }

        LOGGER_FIXED.info(sb.toString());
    }

    /**
     * Prints @msg as print info message
     * 
     * @param msg
     */
    public static void printInfoMsg(String msg) {
        // Separator line
        LOGGER_FIXED.info("");

        // Message line
        StringBuilder sb = new StringBuilder("--- ");
        sb.append(msg);
        sb.append(" ");
        for (int i = PRE_CHARS_MSG_LINE + msg.length(); i < LINE_SIZE; ++i) {
            sb.append("-");
        }

        LOGGER_FIXED.info(sb.toString());
    }
}
