package testPscoStreams;

import es.bsc.distrostreamlib.api.pscos.PscoDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;
import es.bsc.distrostreamlib.exceptions.RegistrationException;
import es.bsc.distrostreamlib.types.ConsumerMode;

import java.io.IOException;
import java.util.List;

import model.Person;


public class Main {

    public static final int NUM_BATCHES = 2;
    public static final int NUM_OBJECTS = 10;

    private static final int SLEEP_TIME1 = 400; // ms
    private static final int SLEEP_TIME2 = 1_000; // ms
    private static final int SLEEP_TIME3 = 2_000; // ms

    private static final int MAX_ENTRIES = 100; // Maximum number of entries for task consumer spawn

    private static final String ALIAS = "custom-stream";


    public static void main(String[] args) throws Exception {
        // One producer, One consumer. ConsumerTime < ProducerTime
        produceConsume(1, SLEEP_TIME2, 1, SLEEP_TIME1);

        // One producer, One consumer. ConsumerTime < ProducerTime. Producer publishes batch of objects
        produceListConsume(SLEEP_TIME2, 1, SLEEP_TIME1);

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
    }

    private static void produceConsume(int numProducers, int producerSleep, int numConsumers, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Create stream
        PscoDistroStream<Person> pds = new PscoDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer tasks
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writePscos(pds, producerSleep);
            // To avoid all the executions start exactly at the same time
            try {
                Thread.sleep(producerSleep / numProducers);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Create consumer tasks
        Integer[] partialPscos = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            partialPscos[i] = Tasks.readPscos(pds, consumerSleep);
            // To avoid all the executions start exactly at the same time
            try {
                Thread.sleep(consumerSleep / numConsumers);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Integer totalPscos = 0;
        for (int i = 0; i < numConsumers; ++i) {
            totalPscos = totalPscos + partialPscos[i];
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + totalPscos);
    }

    private static void produceListConsume(int producerSleep, int numConsumers, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Create stream
        PscoDistroStream<Person> pds = new PscoDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer task
        Tasks.writePscosList(pds, producerSleep);

        // Create consumer tasks
        Integer[] partialPscos = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            partialPscos[i] = Tasks.readPscos(pds, consumerSleep);
        }
        Integer totalPscos = 0;
        for (int i = 0; i < numConsumers; ++i) {
            totalPscos = totalPscos + partialPscos[i];
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + totalPscos);
    }

    private static void produceSpawnConsumers(int numProducers, int producerSleep, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Create stream
        PscoDistroStream<Person> pds = new PscoDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer task
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writePscos(pds, producerSleep);
        }

        // Check stream status and generate consumers
        Integer[] partialPscos = new Integer[MAX_ENTRIES];
        int pos = 0;
        while (!pds.isClosed()) {
            List<Person> newPscos = pds.poll();
            for (Person p : newPscos) {
                System.out
                    .println("Sending " + p.getID() + ":" + p.getName() + ":" + p.getAge() + " to a process task");
                partialPscos[pos] = Tasks.processPsco(p);
                pos = pos + 1;
            }

            // Sleep between polls
            try {
                Thread.sleep(consumerSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Although the stream is closed, there can still be pending events to process
        List<Person> newPscos = pds.poll();
        for (Person p : newPscos) {
            System.out.println("Sending " + p.getID() + ":" + p.getName() + ":" + p.getAge() + " to a process task");
            partialPscos[pos] = Tasks.processPsco(p);
            pos = pos + 1;
        }

        // Get all processed values
        Integer totalPscos = 0;
        for (int i = 0; i < pos; ++i) {
            if (partialPscos[i] != null) {
                totalPscos = totalPscos + partialPscos[i];
            }
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED PSCOS: " + totalPscos);
    }

    private static void byAlias(int numProducers, int producerSleep, int numConsumers, int consumerSleep)
        throws RegistrationException, IOException, BackendException {

        // Create stream
        PscoDistroStream<Person> pds = new PscoDistroStream<>(ALIAS, ConsumerMode.AT_MOST_ONCE);

        // Create producer tasks
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writePscos(pds, producerSleep);
            // To avoid all the executions start exactly at the same time
            try {
                Thread.sleep(producerSleep / numProducers);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Create a new stream by alias that should receive the messages from the previous one
        PscoDistroStream<Person> pds2 = new PscoDistroStream<>(ALIAS, ConsumerMode.AT_MOST_ONCE);

        // Create consumer tasks
        Integer[] partialObjects = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            partialObjects[i] = Tasks.readPscos(pds2, consumerSleep);
            // To avoid all the executions start exactly at the same time
            try {
                Thread.sleep(consumerSleep / numConsumers);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        Integer totalObjects = 0;
        for (int i = 0; i < numConsumers; ++i) {
            totalObjects = totalObjects + partialObjects[i];
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + totalObjects);
    }
}
