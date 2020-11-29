package nonNativeStreamTest;

import es.bsc.distrostreamlib.api.files.FileDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;
import es.bsc.distrostreamlib.exceptions.RegistrationException;

import java.io.IOException;

import binary.BINARY;


public class Main {

    public static final String TEST_PATH = "/tmp/file_stream/";

    private static final int SLEEP_TIME1 = 500; // ms
    private static final int SLEEP_TIME2 = 1000; // s


    public static void main(String[] args) throws RegistrationException, IOException, BackendException {
        // One producer, One consumer. ConsumerTime < ProducerTime
        produceConsume(1, SLEEP_TIME2, 1, SLEEP_TIME1);

        // Clean test path
        Utils.removeDirectory(TEST_PATH);
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
            BINARY.writeFiles(fds, producerSleep / 1_000); // bash sleep is in s
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

}