package testPSCO;

import java.util.UUID;

import model.Person;


public class Main {
	
	
	public static void main(String[] args) {
		//------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO IN");
		testPSCOIn();
		
		//------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO INOUT");
		//testPSCOInOut();
		
		//------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO RETURN");
		//testPSCOReturn();
		
		//------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO INOUT TASK PERSISTED");
		//testPSCOInOutTaskPersisted();
		
		//------------------------------------------------------------------------
		System.out.println("[LOG] Test PSCO IN RETURN NO TASK PERSISTED");
		//testPSCOReturnNoTaskPersisted();
	}
	
	private static void testPSCOIn() {
		String id = "person_" + UUID.randomUUID().toString();
		
		Person p = new Person("PName1", 1, 1);
		p.makePersistent(id);
		
		MainImpl.taskPSCOIn(p);		
	}
	
	private static void testPSCOInOut() {
		String id = "person_" + UUID.randomUUID().toString();
		
		Person p = new Person("PName2", 2, 2);
		p.makePersistent(id);
		
		MainImpl.taskPSCOInOut(p);
		
		String name = p.getName();
		int age = p.getAge();
		int numC = p.getNumComputers();
		System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");
		System.out.println("[LOG] BeginId = " + id + " EndId = " + p.getID());
	}
	
	private static void testPSCOInOutTaskPersisted() {
		Person p = new Person("PName2", 2, 2);
		
		String id = MainImpl.taskPSCOInOutTaskPersisted(p);
		
		String name = p.getName();
		int age = p.getAge();
		int numC = p.getNumComputers();
		System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");
		System.out.println("[LOG] BeginId = " + id + " EndId = " + p.getID());
	}

	private static void testPSCOReturn() {
		String id = "person_" + UUID.randomUUID().toString();
		Person p = MainImpl.taskPSCOReturn("PName3", 3, 3, id);
		
		String name = p.getName();
		int age = p.getAge();
		int numC = p.getNumComputers();
		System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");
		System.out.println("[LOG] BeginId = " + id+ " EndId = " + p.getID());
	}
	
	private static void testPSCOReturnNoTaskPersisted() {
		Person p = MainImpl.taskPSCOReturnNoTaskPersisted("PName3", 3, 3);
		
		String name = p.getName();
		int age = p.getAge();
		int numC = p.getNumComputers();
		System.out.println("[LOG] Person " + name + " with age " + age + " has " + numC + " computers");
		System.out.println("[LOG] BeginId = null EndId = " + p.getID());
	}
	
} 
