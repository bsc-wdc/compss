package streams.components;

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import streams.components.exceptions.InvalidCredentialsException;
import streams.components.exceptions.SubscribeException;
import streams.components.utils.RegistrationId;
import streams.exceptions.ConsumerException;
import streams.types.Messages;
import streams.types.Result;
import streams.types.Topics;


public class Consumer {

    private static final boolean DEBUG = false;
    private static final int MESSAGE_TIMEOUT = 200; // ms


    /**
     * Process the stream messages until and EOS is received
     * 
     * @param stream
     * @return
     */
    public static Result receiveMessages(COMPSsStream stream) {
        // Subscribe
        if (DEBUG) {
            System.out.println("[LOG] Subscribing to stream");
        }
        RegistrationId id;
        try {
            id = stream.subscribe(Topics.ALL_TOPICS);
        } catch (SubscribeException se) {
            System.err.println("ERROR: Cannot subscribe to stream");
            se.printStackTrace();
            return Result.generate(-1);
        }

        // Process stream
        if (DEBUG) {
            System.out.println("[LOG] Processing stream records");
        }
        try {
            boolean end = false;
            while (!end) {
                // Read all the records within the timeout
                // We don't care about timeout because we will receive an end message
                ConsumerRecords<String, String> records = stream.poll(id, MESSAGE_TIMEOUT);

                // Process the received records
                end = processRecords(records);
            }
        } catch (InvalidCredentialsException ice) {
            System.err.println("ERROR: Cannot receive messages from stream");
            ice.printStackTrace();
            return Result.generate(-2);
        } catch (ConsumerException ce) {
            System.err.println("ERROR: Cannot process received records");
            ce.printStackTrace();
            return Result.generate(-3);
        } finally {
            // Close stream
            if (DEBUG) {
                System.out.println("[LOG] Closing stream");
            }
            try {
                stream.unsubscribe(id);
            } catch (InvalidCredentialsException ice) {
                System.err.println("ERROR: Cannot unsubscribe from stream");
                ice.printStackTrace();
                return Result.generate(-4);
            }
        }

        // Set all ok on result
        if (DEBUG) {
            System.out.println("[LOG] DONE");
        }

        // Return result
        return Result.generate(0);
    }

    private static boolean processRecords(ConsumerRecords<String, String> records) throws ConsumerException {
        boolean endRecordReceived = false;
        for (ConsumerRecord<String, String> record : records) {
            switch (record.topic()) {
                case Topics.TOPIC_REGULAR_MESSAGES:
                    processRegularRecord(record.value());
                    break;
                case Topics.TOPIC_SYSTEM_MESSAGES:
                    endRecordReceived = processSystemRecord(record.value());
                    break;
                default:
                    throw new ConsumerException("ERROR: Unrecognised topic " + record.topic());
            }
        }

        return endRecordReceived;
    }

    private static void processRegularRecord(String value) throws ConsumerException {
        // Read the encoded information in the message
        JsonNode msg = null;
        try {
            msg = new ObjectMapper().readTree(value);
        } catch (JsonProcessingException jpe) {
            jpe.printStackTrace();
            throw new ConsumerException("ERROR: Cannot read regular message", jpe);
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new ConsumerException("ERROR: Cannot read regular message", ioe);
        }

        // Retrieve the message type
        switch (msg.get(Messages.FIELD_TYPE).asText()) {
            case Messages.TYPE_MESSAGE:
                long latency = (long) ((System.nanoTime() * 1e-9 - msg.get(Messages.FIELD_T).asDouble()) * 1_000);
                Result.addValue(latency);
                break;
            case Messages.TYPE_STATS:
                // Whenever we receive a stats message we log its retrieval
                if (DEBUG) {
                    System.out.println("[LOG] Stats message received");
                }
                break;
            default:
                throw new ConsumerException("ERROR: Unrecognised message type: " + msg.get(Messages.FIELD_TYPE).asText());
        }
    }

    private static boolean processSystemRecord(String value) throws ConsumerException {
        // Read the encoded information in the message
        JsonNode msg = null;
        try {
            msg = new ObjectMapper().readTree(value);
        } catch (JsonProcessingException jpe) {
            throw new ConsumerException("ERROR: Cannot read system message", jpe);
        } catch (IOException ioe) {
            throw new ConsumerException("ERROR: Cannot read system message", ioe);
        }

        // Retrieve the message type
        switch (msg.get(Messages.FIELD_TYPE).asText()) {
            case Messages.TYPE_END:
                return true;
            default:
                throw new ConsumerException("ERROR: Unrecognised message type: " + msg.get(Messages.FIELD_TYPE).asText());
        }
    }

}
