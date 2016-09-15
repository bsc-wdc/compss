package testPSCOExternal;

import java.util.LinkedList;
import java.util.UUID;

import model.Person;


public class External {

    public static void main(String[] args) {
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO TARGET");
        testPSCOTarget();
        
        // ------------------------------------------------------------------------
        System.out.println("[LOG] Test PSCO MERGE-REDUCE WITH TARGET");
        // TODO: Enable test reduce with target
        //testMergeReduceTarget();
    }

    private static void testPSCOTarget() {
        String id = "person_" + UUID.randomUUID().toString();
        Person p = new Person("PName1", 1, 1);
        p.makePersistent(id);

        // Create mergeable p2
        String id2 = "person_" + UUID.randomUUID().toString();
        Person p2 = new Person("PName2", 1, 2);
        p2.makePersistent(id2);

        p.taskPSCOTargetWithParams("Another", p2);

        String name = p.getName();
        int age = p.getAge();
        int numC = p.getNumComputers();
        System.out.println("[LOG][PSCO_TARGET] Person " + name + " with age " + age + " has " + numC + " computers");
        System.out.println("[LOG][PSCO_TARGET] BeginId = " + id + " EndId = " + p.getID());
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
