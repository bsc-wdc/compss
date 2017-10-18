package testPSCOInternal;

import java.util.UUID;
import model.Computer;
import model.Person;


public class InternalImpl {

    public static void taskPSCOIn(Person p) {
        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");
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
        String pId = p.getID();
        p.deletePersistent();
        p.makePersistent(pId);
    }

    public static Person taskPSCOReturn(String name, int age, int numC, String id) {
        Person p = new Person(name, age, numC);
        p.makePersistent(id);

        // Manually persist object to storage
        String pId = p.getID();
        p.deletePersistent();
        p.makePersistent(pId);

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
        String pId = p.getID();
        p.deletePersistent();
        p.makePersistent(pId);

        return id;
    }

    public static Person taskPSCOReturnNoTaskPersisted(String name, int age, int numC) {
        Person p = new Person(name, age, numC);
        return p;
    }

    public static Person taskMap(String newName, Person p) {
        p.setName(newName);

        String pId = p.getID();
        p.deletePersistent();
        p.makePersistent(pId);

        return p;
    }

    public static Person taskReduce(Person p1, Person p2) {
        p1.setName(p1.getName() + "," + p2.getName());
        p1.setAge(p1.getAge() + p2.getAge());
        for (Computer c : p2.getComputers()) {
            p1.addComputer(c);
        }

        // Manually persist object to storage
        String pId = p1.getID();
        p1.deletePersistent();
        p1.makePersistent(pId);

        return p1;
    }

}
