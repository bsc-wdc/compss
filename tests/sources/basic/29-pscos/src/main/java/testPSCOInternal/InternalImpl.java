package testPSCOInternal;

import java.util.UUID;

import storage.StorageException;
import storageManager.StorageManager;
import model.Computer;
import model.Person;


public class InternalImpl {

    private static final String ERROR_PERSIST = "[ERROR] Cannot persist object";


    public static void taskPSCOIn(Person p) {
        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");

        // Manually persist object to storage
        try {
            StorageManager.persist(p);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    public static void taskPSCOInOut(Person p) {
        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");

        p.setName("Another");
        p.setAge(10);
        Computer c = new Computer("DELL", "Latitude", name + "_" + age, age);
        p.addComputer(c);

        name = p.getName();
        age = p.getAge();
        numC = p.getNumComputers();
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");

        // Manually persist object to storage
        try {
            StorageManager.persist(p);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    public static Person taskPSCOReturn(String name, int age, int numC, String id) {
        Person p = new Person(name, age, numC);
        p.makePersistent(id);

        // Manually persist object to storage
        try {
            StorageManager.persist(p);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }

        return p;
    }

    public static String taskPSCOInOutTaskPersisted(Person p) {
        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");

        p.setName("Another");
        p.setAge(10);
        Computer c = new Computer("DELL", "Latitude", name + "_" + age, age);
        p.addComputer(c);

        String id = "person_" + UUID.randomUUID().toString();
        p.makePersistent(id);

        // Manually persist object to storage
        try {
            StorageManager.persist(p);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }

        return id;
    }

    public static Person taskPSCOReturnNoTaskPersisted(String name, int age, int numC) {
        Person p = new Person(name, age, numC);
        return p;
    }

    public static Person taskMap(String newName, Person p) {
        p.setName(newName);

        // Manually persist object to storage
        try {
            StorageManager.persist(p);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }

        return p;
    }

    public static Person taskReduce(Person p1, Person p2) {
        p1.setName(p1.getName() + "," + p2.getName());
        p1.setAge(p1.getAge() + p2.getAge());
        for (Computer c : p2.getComputers()) {
            p1.addComputer(c);
        }

        // Manually persist object to storage
        try {
            StorageManager.persist(p1);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }

        return p1;
    }

}
