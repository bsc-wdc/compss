package testPSCO;

import java.util.UUID;

import model.Computer;
import model.Person;


public class MainImpl {

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
		return id;
	}

	public static Person taskPSCOReturn(String name, int age, int numC, String id) {
		Person p = new Person(name, age, numC);
		p.makePersistent(id);
		
		return p;
	}

	public static Person taskPSCOReturnNoTaskPersisted(String name, int age, int numC) {
		Person p = new Person(name, age, numC);		
		return p;
	}

}
