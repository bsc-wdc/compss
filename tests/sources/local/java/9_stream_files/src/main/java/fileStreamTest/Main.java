package fileStreamTest;

import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;
import es.bsc.distrostreamlib.exceptions.RegistrationException;

import java.io.IOException;
import java.util.List;


public class Main {

    public static final String TEST_PATH = "/tmp/file_stream/";
    public static final String BASE_FILENAME = "file";

    public static final String ALIAS = "custom-stream";
    public static final String TEST_PATH_ALIAS = "/tmp/file_custom_stream/";

    public static final int NUM_FILES = 10;

    private static final int SLEEP_TIME1 = 400; // ms
    private static final int SLEEP_TIME2 = 1_000; // ms
    private static final int SLEEP_TIME3 = 2_000; // ms

    private static final int MAX_ENTRIES = 100; // Maximum number of entries for task consumer spawn


    public static void main(String[] args) throws RegistrationException, IOException, BackendException {
        // One producer, One consumer. ConsumerTime < ProducerTime
        produceConsume(1, SLEEP_TIME2, 1, SLEEP_TIME1);

        // One producer, One consumer. ConsumerTime > ProducerTime
        produceConsume(1, SLEEP_TIME2, 1, SLEEP_TIME3);

        // Two producer, One consumer
        produceConsume(2, SLEEP_TIME2, 1, SLEEP_TIME1);

        // One producer, Two consumer
        produceConsume(1, SLEEP_TIME1, 2, SLEEP_TIME2);

        // Two producer, Two consumer
        produceConsume(2, SLEEP_TIME2, 2, SLEEP_TIME1);

        // One producer task, main consumer generating tasks
        produceSpawnConsumers(1, SLEEP_TIME1, SLEEP_TIME2);

        // By alias
        byAlias(1, SLEEP_TIME2, 1, SLEEP_TIME1);

        // Clean test path
        Utils.removeDirectory(TEST_PATH);
        Utils.removeDirectory(TEST_PATH_ALIAS);
    }

    private static void produceConsume(int numProducers, int producerSleep, int numConsumers, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Clean and create base test path
        Utils.removeDirectory(TEST_PATH);
        Utils.createDirectory(TEST_PATH);

        // Create stream
        FileDistroStream fds = new FileDistroStream(TEST_PATH);

        // Create producer task
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writeFiles(fds, producerSleep);
        }

        // Create consumer task
        Integer[] totalFiles = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            totalFiles[i] = Tasks.readFiles(fds, consumerSleep);
        }
        Integer total = 0;
        for (Integer partial : totalFiles) {
            total = total + partial;
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED FILES: " + total);
    }

    private static void produceSpawnConsumers(int numProducers, int producerSleep, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Clean and create base test path
        Utils.removeDirectory(TEST_PATH);
        Utils.createDirectory(TEST_PATH);

        // Create stream
        FileDistroStream fds = new FileDistroStream(TEST_PATH);

        // Create producer task
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writeFiles(fds, producerSleep);
        }

        // Check stream status and generate consumers
        Integer[] totalFiles = new Integer[MAX_ENTRIES];
        int pos = 0;
        while (!fds.isClosed()) {
            // Sleep between polls
            try {
                Thread.sleep(consumerSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Poll new files
            List<String> newFiles = fds.poll();

            // Process their content
            for (String fileName : newFiles) {
                System.out.println("Sending " + fileName + " to a process task");
                totalFiles[pos] = Tasks.processFile(fileName);
                pos = pos + 1;
            }

        }
        // Sleep between polls
        try {
            Thread.sleep(consumerSleep);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Although the stream is closed, there can still be pending events to process
        List<String> newFiles = fds.poll();
        // Process their content
        for (String fileName : newFiles) {
            System.out.println("Sending " + fileName + " to a process task");
            totalFiles[pos] = Tasks.processFile(fileName);
            pos = pos + 1;
        }

        // Count total
        Integer total = 0;
        for (int i = 0; i < pos; i++) {
            Integer partial = totalFiles[i];
            if (partial != null) {
                total = total + partial;
            }
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED FILES: " + total);
    }

    private static void byAlias(int numProducers, int producerSleep, int numConsumers, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Clean and create base test path
        Utils.removeDirectory(TEST_PATH_ALIAS);
        Utils.createDirectory(TEST_PATH_ALIAS);

        // Create stream
        FileDistroStream fds = new FileDistroStream(ALIAS, TEST_PATH_ALIAS);

        // Create producer task
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writeFilesAlias(fds, producerSleep);
        }

        // Create a new stream by alias that should receive the messages from the previous one
        FileDistroStream fds2 = new FileDistroStream(ALIAS, TEST_PATH_ALIAS);

        // Create consumer task
        Integer[] totalFiles = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            totalFiles[i] = Tasks.readFiles(fds2, consumerSleep);
        }
        Integer total = 0;
        for (Integer partial : totalFiles) {
            total = total + partial;
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED FILES: " + total);
    }

}