package testPSCOExternal;

import java.util.UUID;

import model.Person;


public class External {

	public static void main(String[] args) {		
		// ------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO TARGET");
		testPSCOTarget();
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
	
}
