package objectStreamTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import es.bsc.distrostreamlib.api.objects.ObjectDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;


public class Tasks {

    private static final String BASE_NAME = "CUSTOM_OBJECT_";


    public static void writeObjects(ObjectDistroStream<MyObject> ods, int sleepTime) throws BackendException {
        // Create several new files and add them to the stream when written
        for (int i = 0; i < Main.NUM_OBJECTS; ++i) {
            // Sleep some time to delay production
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Create new object
            String name = BASE_NAME + UUID.randomUUID();
            MyObject obj = new MyObject(name, i);

            // Publish to stream
            System.out.println("SENDING OBJECT: " + obj.hashCode());
            System.out.println(" - Name: " + obj.getName());
            System.out.println(" - Value: " + obj.getValue());
            ods.publish(obj);

        }

        // Send end event when finished
        System.out.println("Requesting stream closure");
        try {
            Thread.sleep(sleepTime * 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ods.close();
    }

    public static void writeObjectList(ObjectDistroStream<MyObject> ods, int sleepTime) throws BackendException {
        // Create several new files and add them to the stream when written
        for (int i = 0; i < Main.NUM_BATCHES; ++i) {
            // Sleep some time to delay production
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // Create list of objects
            List<MyObject> objects = new ArrayList<>();
            for (int j = 0; j < Main.NUM_OBJECTS; ++j) {
                // Create new object
                String name = BASE_NAME + UUID.randomUUID();
                MyObject obj = new MyObject(name, i * Main.NUM_OBJECTS + j);
                objects.add(obj);
            }

            // Publish to stream
            System.out.println("SENDING OBJECTS: " + objects.hashCode());
            for (MyObject obj : objects) {
                System.out.println(" - Name: " + obj.getName());
                System.out.println(" - Value: " + obj.getValue());
            }
            ods.publish(objects);

        }
        // Sleep some time to delay production
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Send end event when finished
        System.out.println("Requesting stream closure");
        ods.close();
    }

    public static Integer readObjects(ObjectDistroStream<MyObject> ods, int sleepTime)
        throws IOException, BackendException {

        // Process events until stream is closed
        Integer totalObjects = 0;
        while (!ods.isClosed()) {
            // Sleep between polls
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Polling new objects");
            Integer numNewObjects = pollObjects(ods);
            totalObjects = totalObjects + numNewObjects;
        }
        // Sleep between polls
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Although the stream is closed, there can still be pending events to process
        Integer numNewObjects = pollObjects(ods);
        totalObjects = totalObjects + numNewObjects;

        return totalObjects;
    }

    public static Integer readObjectsTimeout(ObjectDistroStream<MyObject> ods, long timeout, int sleepTime)
        throws IOException, BackendException {

        // Process events until stream is closed
        Integer totalObjects = 0;
        while (!ods.isClosed()) {
            // Sleep between polls
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            System.out.println("Polling new objects");
            Integer numNewObjects = pollWithTimeout(ods, timeout);
            totalObjects = totalObjects + numNewObjects;

        }
        // Sleep between polls
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // Although the stream is closed, there can still be pending events to process
        Integer numNewObjects = pollWithTimeout(ods, timeout);
        totalObjects = totalObjects + numNewObjects;

        return totalObjects;
    }

    private static Integer pollObjects(ObjectDistroStream<MyObject> ods) throws IOException, BackendException {
        // Poll new files
        List<MyObject> newObjects = ods.poll();

        // Process their content
        for (MyObject obj : newObjects) {
            System.out.println("RECEIVED OBJECT: " + obj.hashCode());
            System.out.println(" - Name: " + obj.getName());
            System.out.println(" - Value: " + obj.getValue());
        }

        // Return the number of processed objects
        return newObjects.size();
    }

    private static Integer pollWithTimeout(ObjectDistroStream<MyObject> ods, long timeout)
        throws IOException, BackendException {

        // Poll new files
        List<MyObject> newObjects = ods.poll(timeout);

        // Process their content
        for (MyObject obj : newObjects) {
            System.out.println("RECEIVED OBJECT: " + obj.hashCode());
            System.out.println(" - Name: " + obj.getName());
            System.out.println(" - Value: " + obj.getValue());
        }

        // Return the number of processed objects
        return newObjects.size();
    }

    public static Integer processObject(MyObject obj) throws IOException {
        System.out.println("RECEIVED OBJECT: " + obj.hashCode());
        System.out.println(" - Name: " + obj.getName());
        System.out.println(" - Value: " + obj.getValue());

        return 1;
    }
}
