package testPSCOInternal;

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

}
