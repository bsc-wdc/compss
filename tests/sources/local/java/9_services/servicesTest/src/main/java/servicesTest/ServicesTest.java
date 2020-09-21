package servicesTest;

import groupservice.Person;
import dummy.groupService.GroupService.GroupServicePort.GroupService;


public class ServicesTest {

    private static int numCalls;
    private static GroupService gs = new GroupService();


    public static void main(String[] args) {
        // Check and get parameters
        if (args.length != 1) {
            System.out.println("[ERROR] Bad usage. Call servicesTest <numCalls>");
            System.exit(-1);
        }
        numCalls = Integer.parseInt(args[0]);

        // Run stateless test
        System.out.println("[LOG] Checking service stateless dependencies.");
        serviceStatelessTest();

        // Instantiate and initialize service
        System.out.println();
        System.out.println("[LOG] Initialising service for future statefull calls.");
        gs.setNumWorkers(numCalls);

        // Run statefull test
        System.out.println();
        System.out.println("[LOG] Checking service statefull dependencies.");
        serviceStatefullTest();

        // Run service-method test
        System.out.println();
        System.out.println("[LOG] Checking service-method dependencies.");
        serviceMethodTest();

        // Run method-service test
        System.out.println();
        System.out.println("[LOG] Checking method-service dependencies.");
        methodServiceTest();

        System.out.println();
        System.out.println("[LOG] All tests finished.");
        System.out.println("[LOG] No more jobs for main.");
        System.out.println("[LOG] Result must be checked by result_script");
    }

    private static void serviceStatelessTest() { // All calls are parallel s,s,s
        System.out.println("[LOG] -- Making " + numCalls + " stateless calls");
        for (int i = 0; i < numCalls; ++i) {
            GroupService.Static.getOwner();
        }
        System.out.println("[LOG] -- All calls created. Test finished");
    }

    private static void serviceStatefullTest() { // All calls should be in line s-->s
        // Make statefull calls
        System.out.println("[LOG] -- Making " + numCalls + " statefull calls");
        for (int i = 0; i < numCalls; ++i) {
            // Define auxiliar person
            Person p = new Person();
            p.setName("You");
            p.setSurname("You");
            p.setDni("87654321Z");
            p.setAge(25);
            p.setProduction((int) (Math.random() * 10.0));
            p.setWorkingHours(4 + (int) (Math.random() * 4.0));
            gs.setWorker(p, i);
        }

        // Check statefull value
        double prod = gs.productivity();
        System.out.println("[LOG] -- Productivity obtained ss: " + String.format("%1.4f", prod));
        System.out.println("[LOG] -- This productivity should be equal to print's productivity");
    }

    private static void serviceMethodTest() { // All calls are parallel in line s-->m, s-->m, s-->m
        System.out.println("[LOG] -- Making a stateless call as method parameter");
        ServicesTestImpl.print(GroupService.Static.getOwner());

        System.out.println("[LOG] -- Making " + numCalls + " statefull calls as method parameter");
        for (int i = 0; i < numCalls; ++i) {
            ServicesTestImpl.print(gs.getWorker(i));
        }

        System.out.println("[LOG] -- Service Method Test finished");
    }

    /*
     * TODO private static void serviceMethodTestWithSync() { //All calls are parallel in line s-->m, s-->m, s-->m
     * System.out.println("[LOG] -- Making a stateless call as method parameter");
     * ServicesTestImpl.print(GroupService.Static.getOwner());
     * 
     * System.out.println("[LOG] -- Making " + numCalls + " statefull calls as method parameter"); for (int i = 0; i <
     * numCalls; ++i) { Person p = gs.getWorker(i); System.out.println("  - Person Age (SYNC) = " + p.getAge());
     * ServicesTestImpl.print(p); }
     * 
     * System.out.println("[LOG] -- Service Method Test finished"); }
     */

    private static void methodServiceTest() { // All calls are parallel in line m-->s, m-->s, m-->s
        System.out.println("[LOG] -- Calling methods with service calls structure inside");
        for (int i = 0; i < numCalls; ++i) {
            gs.setWorker(ServicesTestImpl.createPerson(), i);
        }
        double prod = gs.productivity();
        System.out.println("[LOG] -- Productivity obtained ms: " + String.format("%1.4f", prod));
        System.out.println("[LOG] -- This productivity should be equal to createPerson's productivity");
        System.out.println("[LOG] -- All calls for this tests done. Waiting methods to finish.");
    }

}
