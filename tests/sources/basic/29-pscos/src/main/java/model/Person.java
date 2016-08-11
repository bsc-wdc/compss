package model;

import java.io.Serializable;
import java.util.LinkedList;

import storage.StorageObject;

public class Person extends StorageObject implements Serializable {
	
	/**
	 * Serial ID for Objects outside the runtime
	 */
	private static final long serialVersionUID = 3L;
	
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

}
