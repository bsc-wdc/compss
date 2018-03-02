package streams.components;

import java.util.Locale;

import streams.components.exceptions.AnnounceException;
import streams.components.exceptions.InvalidCredentialsException;
import streams.components.utils.RegistrationId;
import streams.types.Messages;
import streams.types.Topics;


public class Producer {

    private static final int FREQ_STATS_MSG = 1_000;
    private static final int FREQ_WAIT_TIME = 10_000;
    private static final int WAIT_TIME = 200; // ms

    private static final boolean DEBUG = false;


    /**
     * Sends numMessages messages trough the given stream and closes it
     * 
     * @param stream
     * @param numMessages
     * @return
     */
    public static Integer sendMessages(COMPSsStream stream, int numMessages) {
        // Announce on stream
        if (DEBUG) {
            System.out.println("[LOG] Announcing stream write");
        }
        RegistrationId id = null;
        try {
            id = stream.announce();
        } catch (AnnounceException ae) {
            System.err.println("ERROR: Cannot announce publication");
            ae.printStackTrace();
            return -1;
        }

        // Send messages
        if (DEBUG) {
            System.out.println("[LOG] Sending " + numMessages + " messages");
        }
        for (int i = 0; i < numMessages; ++i) {
            // Send lots of regular messages with type message
            // Message of the form {"type":"message", t=12.234, k=1}
            String msg = String.format(Locale.ROOT, "{\"" + Messages.FIELD_TYPE + "\":\"" + Messages.TYPE_MESSAGE + "\", \""
                    + Messages.FIELD_T + "\":%.3f, \"" + Messages.FIELD_K + "\":%d}", System.nanoTime() * 1e-9, i);
            try {
                stream.publish(id, Topics.TOPIC_REGULAR_MESSAGES, msg);
            } catch (InvalidCredentialsException ice) {
                System.err.println("ERROR: Cannot send regular message");
                ice.printStackTrace();
                return -1;
            }

            // Every so often send a stats message
            if (i % FREQ_STATS_MSG == 0) {
                String statMsg = String.format(Locale.ROOT, "{\"" + Messages.FIELD_TYPE + "\":\"" + Messages.TYPE_STATS + "\", \""
                        + Messages.FIELD_T + "\":%.3f, \"" + Messages.FIELD_K + "\":%d}", System.nanoTime() * 1e-9, i);
                try {
                    stream.publish(id, Topics.TOPIC_REGULAR_MESSAGES, statMsg);
                } catch (InvalidCredentialsException ice) {
                    System.err.println("ERROR: Cannot send stat message");
                    ice.printStackTrace();
                    return -1;
                }
            }

            // Every so often send a sleep for a while
            if (i % FREQ_WAIT_TIME == 0) {
                try {
                    Thread.sleep(WAIT_TIME);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // Send end message
        if (DEBUG) {
            System.out.println("[LOG] Sending end messge");
        }
        String endMsg = String.format(Locale.ROOT, "{\"" + Messages.FIELD_TYPE + "\":\"" + Messages.TYPE_END + "\", \"" + Messages.FIELD_T
                + "\":%.3f, \"" + Messages.FIELD_K + "\":%d}", System.nanoTime() * 1e-9, 0);
        try {
            stream.publish(id, Topics.TOPIC_SYSTEM_MESSAGES, endMsg);
        } catch (InvalidCredentialsException ice) {
            System.err.println("ERROR: Cannot send end message");
            ice.printStackTrace();
            return -1;
        }

        // Close stream
        if (DEBUG) {
            System.out.println("[LOG] Closing stream");
        }
        try {
            stream.close(id);
        } catch (InvalidCredentialsException ice) {
            System.err.println("ERROR: Cannot close stream");
            ice.printStackTrace();
            return -1;
        }

        // All ok
        if (DEBUG) {
            System.out.println("[LOG] DONE");
        }
        return 0;
    }

}
