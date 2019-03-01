package model;

import java.io.Serializable;
import java.util.LinkedList;

import storage.StorageException;
import storage.StorageObject;
import storageManager.StorageManager;


public class Person extends StorageObject implements Serializable {

    /**
     * Serial ID for Objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    private static final String ERROR_PERSIST = "[ERROR] Cannot persist object";

    private String name;
    private int age;
    private final LinkedList<Computer> computers = new LinkedList<Computer>();


    public Person() {
        super();
    }

    public Person(String name, int age, int numC) {
        super();
        this.name = name;
        this.age = age;
        for (int i = 0; i < numC; ++i) {
            String compId = name + "_" + String.valueOf(i);
            Computer c = new Computer("DELL", "Latitude", compId, age);
            this.computers.add(c);
        }
    }

    public String getName() {
        return this.name;
    }

    public int getAge() {
        return this.age;
    }

    public LinkedList<Computer> getComputers() {
        return this.computers;
    }

    public int getNumComputers() {
        return this.computers.size();
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void addComputer(Computer c) {
        this.computers.add(c);
    }

    // Task
    public void taskPSCOTarget() {
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        this.setName("Another");
        this.setAge(10);
        Computer c = new Computer("DELL", "Latitude", name + "_" + age, age);
        this.addComputer(c);

        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    // Task
    public void taskPSCOTargetTaskPersisted(String id) {
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        this.setName("Another");
        this.setAge(10);
        Computer c = new Computer("DELL", "Latitude", name + "_" + age, age);
        this.addComputer(c);

        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        // Persist
        this.makePersistent(id);

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    // Task
    public void taskPSCOTargetWithParams(String newName, Person p) {
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        this.setName(newName);
        this.setAge(10);
        Computer c = new Computer("DELL", "Latitude", name + "_" + age, age);
        this.addComputer(c);

        // Merge person p computers
        for (Computer computer : p.getComputers()) {
            this.addComputer(computer);
        }

        System.out.println("[LOG] Person " + name + " with age " + age + " has " + computers.size() + " computers");

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }
    
    // Task
    public void taskMap(String newName) {
        this.setName(newName);

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

    // Task
    public void taskReduce(Person p2) {
        this.setName(this.getName() + "," + p2.getName());
        this.setAge(this.getAge() + p2.getAge());
        for (Computer c : p2.getComputers()) {
            this.addComputer(c);
        }

        // Manually persist object to storage
        try {
            StorageManager.persist(this);
        } catch (StorageException e) {
            System.err.println(ERROR_PERSIST);
            e.printStackTrace();
        }
    }

}
