package testPSCOInternal;

import java.util.LinkedList;
import java.util.UUID;

import model.Person;


public class Internal {

    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO IN");
        testPSCOIn();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO INOUT");
        testPSCOInOut();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO RETURN");
        testPSCOReturn();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO TARGET");
        testPSCOTarget();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO INOUT TASK PERSISTED");
        testPSCOInOutTaskPersisted();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO IN RETURN NO TASK PERSISTED");
        testPSCOReturnNoTaskPersisted();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO TARGET TASK PERSISTED");
        testPSCOTargetTaskPersisted();

        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO MERGE-REDUCE");
        testMergeReduce();
        
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO MERGE-REDUCE WITH TARGET");
        testMergeReduceTarget();
    }

    private static void testPSCOIn() {
        String id = "person_" + UUID.randomUUID().toString();

        Person p = new Person("PName1", 1, 1);
        p.makePersistent(id);

        InternalImpl.taskPSCOIn(p);
    }

    private static void testPSCOInOut() {
        String id = "person_" + UUID.randomUUID().toString();

        Person p = new Person("PName2", 2, 2);
        p.makePersistent(id);

        InternalImpl.taskPSCOInOut(p);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_INOUT] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_INOUT] BeginId = " + id + " EndId = " + p.getID());
    }

    private static void testPSCOInOutTaskPersisted() {
        Person p = new Person("PName2", 2, 2);

        String id = InternalImpl.taskPSCOInOutTaskPersisted(p);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_INOUT_TP] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_INOUT_TP] BeginId = " + id + " EndId = " + p.getID());
    }

    private static void testPSCOReturn() {
        String id = "person_" + UUID.randomUUID().toString();
        Person p = InternalImpl.taskPSCOReturn("PName3", 3, 3, id);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_RETURN] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_RETURN] BeginId = " + id + " EndId = " + p.getID());
    }

    private static void testPSCOReturnNoTaskPersisted() {
        Person p = InternalImpl.taskPSCOReturnNoTaskPersisted("PName3", 3, 3);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_RETURN_NTP] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_RETURN_NTP] BeginId = null EndId = " + p.getID());
    }

    private static void testPSCOTarget() {
        String id = "person_" + UUID.randomUUID().toString();
        Person p = new Person("PName1", 1, 1);
        p.makePersistent(id);

        // Invoke 2 times to check if parameter is well returned from worker
        p.taskPSCOTarget();
        p.taskPSCOTarget();

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_TARGET] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_TARGET] BeginId = " + id + " EndId = " + p.getID());
    }

    private static void testPSCOTargetTaskPersisted() {
        String id = "person_" + UUID.randomUUID().toString();
        Person p = new Person("PName1", 1, 1);

        p.taskPSCOTargetTaskPersisted(id);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_TARGET_TP] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_TARGET_TP] BeginId = null EndId = " + p.getID());
    }

    public static void testMergeReduce() {
        // Init
        Person[] people = new Person[4];
        for (int i = 0; i < people.length; ++i) {
            String id = "person_" + UUID.randomUUID().toString();
            System.out.println("[LOG][PSCO_MR] Person " + i + " BeginId = " + id);
            people[i] = new Person("PName" + i, i, i);
            people[i].makePersistent(id);
        }

        // Map
        for (int i = 0; i < people.length; ++i) {
            people[i] = InternalImpl.taskMap("NewName" + i, people[i]);
        }

        // Reduce
        LinkedList<Integer> q = new LinkedList<Integer>();
        for (int i = 0; i < people.length; i++) {
            q.add(i);
        }
        int x = 0;
        while (!q.isEmpty()) {
            x = q.poll();
            int y;
            if (!q.isEmpty()) {
                y = q.poll();
                people[x] = InternalImpl.taskReduce(people[x], people[y]);
                q.add(x);
            }
        }

        // Get (sync) and write result
        Person p1 = people[0];
        String name = p1.getName();
        int age = p1.getAge();
        int numC = p1.getNumComputers();
        System.out.println("[LOG][PSCO_MR] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_MR] EndId = " + p1.getID());
    }
    
    public static void testMergeReduceTarget() {
        // Init
        Person[] people = new Person[4];
        for (int i = 0; i < people.length; ++i) {
            String id = "person_" + UUID.randomUUID().toString();
            System.out.println("[LOG][PSCO_MR_TARGET] Person " + i + " BeginId = " + id);
            people[i] = new Person("PName" + i, i, i);
            people[i].makePersistent(id);
        }

        // Map
        for (int i = 0; i < people.length; ++i) {
            people[i].taskMap("NewName" + i);
        }

        // Reduce
        LinkedList<Integer> q = new LinkedList<Integer>();
        for (int i = 0; i < people.length; i++) {
            q.add(i);
        }
        int x = 0;
        while (!q.isEmpty()) {
            x = q.poll();
            int y;
            if (!q.isEmpty()) {
                y = q.poll();
                people[x].taskReduce(people[y]);
                q.add(x);
            }
        }

        // Get (sync) and write result
        Person p1 = people[0];
        String name = p1.getName();
        int age = p1.getAge();
        int numC = p1.getNumComputers();
        System.out.println("[LOG][PSCO_MR_TARGET] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_MR_TARGET] EndId = " + p1.getID());
    }

}
