package testPscoStreams;

import es.bsc.distrostreamlib.api.pscos.PscoDistroStream;
import es.bsc.distrostreamlib.exceptions.BackendException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import model.Person;

import storage.StorageException;
import storageManager.StorageManager;


public class Tasks {

    private static final String ERROR_PERSIST = "[ERROR] Cannot persist object";
    private static final String BASE_NAME = "CUSTOM_OBJECT_";


    public static void writePscos(PscoDistroStream<Person> pds, int sleepTime) throws BackendException {
        // Create several new files and add them to the stream when written
        for (int i = 0; i < Main.NUM_OBJECTS; ++i) {
            // Create new object
            String name = BASE_NAME + UUID.randomUUID();
            Person p = new Person(name, i);
            String id = "person_" + UUID.randomUUID().toString();
            p.makePersistent(id);

            // Manually persist object to storage
            try {
                StorageManager.persist(p);
            } catch (StorageException e) {
                System.err.println(ERROR_PERSIST);
                e.printStackTrace();
            }

            // Publish to stream
            System.out.println("SENDING PSCO: " + p.getID());
            System.out.println(" - Name: " + p.getName());
            System.out.println(" - Value: " + p.getAge());
            pds.publish(p);

            // Sleep some time to delay production
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Send end event when finished
        System.out.println("Requesting stream closure");
        try {
            Thread.sleep(sleepTime * 2);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        pds.close();
    }

    public static void writePscosList(PscoDistroStream<Person> pds, int sleepTime) throws BackendException {
        // Create several new files and add them to the stream when written
        for (int i = 0; i < Main.NUM_BATCHES; ++i) {
            // Create list of objects
            List<Person> objects = new ArrayList<>();
            for (int j = 0; j < Main.NUM_OBJECTS; ++j) {
                // Create new object
                String name = BASE_NAME + UUID.randomUUID();
                Person p = new Person(name, i);
                String id = "person_" + UUID.randomUUID().toString();
                p.makePersistent(id);

                // Manually persist object to storage
                try {
                    StorageManager.persist(p);
                } catch (StorageException e) {
                    System.err.println(ERROR_PERSIST);
                    e.printStackTrace();
                }

                objects.add(p);
            }

            // Publish to stream
            System.out.println("SENDING PSCOS: ");
            for (Person p : objects) {
                System.out.println(" - Name: " + p.getName());
                System.out.println(" - Value: " + p.getAge());
            }
            pds.publish(objects);

            // Sleep some time to delay production
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Send end event when finished
        System.out.println("Requesting stream closure");
        pds.close();
    }

    public static Integer readPscos(PscoDistroStream<Person> pds, int sleepTime) throws IOException, BackendException {
        // Process events until stream is closed
        Integer totalObjects = 0;
        while (!pds.isClosed()) {
            System.out.println("Polling new pscos");
            Integer numNewObjects = pollPscos(pds);
            totalObjects = totalObjects + numNewObjects;

            // Sleep between polls
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // Although the stream is closed, there can still be pending events to process
        Integer numNewObjects = pollPscos(pds);
        totalObjects = totalObjects + numNewObjects;

        return totalObjects;
    }

    private static Integer pollPscos(PscoDistroStream<Person> pds) throws IOException, BackendException {
        // Poll new files
        List<Person> newObjects = pds.poll();

        // Process their content
        for (Person p : newObjects) {
            System.out.println("RECEIVED PSCO: " + p.getID());
            System.out.println(" - Name: " + p.getName());
            System.out.println(" - Value: " + p.getAge());
        }

        // Return the number of processed objects
        return newObjects.size();
    }

    public static Integer processPsco(Person p) throws IOException {
        System.out.println("RECEIVED PSCO: " + p.getID());
        System.out.println(" - Name: " + p.getName());
        System.out.println(" - Value: " + p.getAge());

        return 1;
    }
}
