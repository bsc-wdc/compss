package objectStreamTest;

import es.bsc.distrostreamlib.api.objects.ObjectDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;
import es.bsc.distrostreamlib.exceptions.RegistrationException;
import es.bsc.distrostreamlib.types.ConsumerMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Main {

    public static final int NUM_BATCHES = 2;
    public static final int NUM_OBJECTS = 10;

    private static final int SLEEP_TIME1 = 400; // ms
    private static final int SLEEP_TIME2 = 1_000; // ms
    private static final int SLEEP_TIME3 = 2_000; // ms


    public static void main(String[] args) throws RegistrationException, IOException, BackendException {
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
    }

    private static void produceConsume(int numProducers, int producerSleep, int numConsumers, int consumerSleep)
            throws RegistrationException, IOException, BackendException {

        // Create stream
        ObjectDistroStream<MyObject> ods = new ObjectDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer tasks
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writeObjects(ods, producerSleep);
            // To avoid all the executions start exactly at the same time
            try {
                Thread.sleep(producerSleep / numProducers);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Create consumer tasks
        Integer[] partialObjects = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            partialObjects[i] = Tasks.readObjects(ods, consumerSleep);
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

    private static void produceListConsume(int producerSleep, int numConsumers, int consumerSleep)
            throws RegistrationException, IOException, BackendException {

        // Create stream
        ObjectDistroStream<MyObject> ods = new ObjectDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer task
        Tasks.writeObjectList(ods, producerSleep);

        // Create consumer tasks
        Integer[] partialObjects = new Integer[numConsumers];
        for (int i = 0; i < numConsumers; ++i) {
            partialObjects[i] = Tasks.readObjects(ods, consumerSleep);
        }
        Integer totalObjects = 0;
        for (int i = 0; i < numConsumers; ++i) {
            totalObjects = totalObjects + partialObjects[i];
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + totalObjects);
    }

    private static void produceSpawnConsumers(int numProducers, int producerSleep, int consumerSleep)
            throws RegistrationException, IOException, BackendException {

        // Create stream
        ObjectDistroStream<MyObject> ods = new ObjectDistroStream<>(ConsumerMode.AT_MOST_ONCE);

        // Create producer task
        for (int i = 0; i < numProducers; ++i) {
            Tasks.writeObjects(ods, producerSleep);
        }

        // Check stream status and generate consumers
        List<Integer> partialObjects = new ArrayList<>();
        while (!ods.isClosed()) {
            Integer polledObjects = pollNewObjectsAndSpawnConsumer(ods);
            partialObjects.add(polledObjects);

            // Sleep between polls
            try {
                Thread.sleep(consumerSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Although the stream is closed, there can still be pending events to process
        Integer polledObjects = pollNewObjectsAndSpawnConsumer(ods);
        partialObjects.add(polledObjects);

        // Get all processed values
        Integer totalObjects = 0;
        for (Integer p : partialObjects) {
            totalObjects = totalObjects + p;
        }

        // Wait for tasks completion
        System.out.println("[LOG] TOTAL NUMBER OF PROCESSED OBJECTS: " + totalObjects);
    }

    private static Integer pollNewObjectsAndSpawnConsumer(ObjectDistroStream<MyObject> ods)
            throws IOException, BackendException {

        System.out.println("[LOG] Polling from main");
        Integer polledObjects = 0;
        // Poll new files
        List<MyObject> newObjects = ods.poll();
        // Process their content
        for (MyObject obj : newObjects) {
            System.out.println(
                    "Sending " + obj.hashCode() + ":" + obj.getName() + ":" + obj.getValue() + " to a process task");
            Integer numProcessed = Tasks.processObject(obj);
            polledObjects = polledObjects + numProcessed;
        }
        return polledObjects;
    }

}