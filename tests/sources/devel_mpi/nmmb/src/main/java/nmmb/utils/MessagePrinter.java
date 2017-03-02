package nmmb.utils;

import org.apache.logging.log4j.Logger;


/**
 * Helper class to print messages
 *
 */
public class MessagePrinter {

    // For info messages
    private static final int LINE_SIZE = 60;
    private static final int PRE_CHARS_HEADER_LINE = 11;
    private static final int PRE_CHARS_MSG_LINE = 5;

    // Loggers
    private final Logger logger;


    /**
     * Creates a message printer for a specific logger
     * 
     * @param logger
     */
    public MessagePrinter(Logger logger) {
        this.logger = logger;
    }

    /**
     * Prints @msg as header message
     * 
     * @param msg
     */
    public void printHeaderMsg(String msg) {
        // Separator line
        logger.info("");

        // Message line
        StringBuilder sb = new StringBuilder("========= ");
        sb.append(msg);
        sb.append(" ");
        for (int i = PRE_CHARS_HEADER_LINE + msg.length(); i < LINE_SIZE; ++i) {
            sb.append("=");
        }

        logger.info(sb.toString());
    }

    /**
     * Prints @msg as print info message
     * 
     * @param msg
     */
    public void printInfoMsg(String msg) {
        // Separator line
        logger.info("");

        // Message line
        StringBuilder sb = new StringBuilder("--- ");
        sb.append(msg);
        sb.append(" ");
        for (int i = PRE_CHARS_MSG_LINE + msg.length(); i < LINE_SIZE; ++i) {
            sb.append("-");
        }

        logger.info(sb.toString());
    }

}
