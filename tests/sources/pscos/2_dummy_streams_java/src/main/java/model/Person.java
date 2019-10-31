package model;

import java.io.Serializable;

import storage.StorageObject;


public class Person extends StorageObject implements Serializable {

    /**
     * Serial ID for Objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    private String name;
    private int age;


    /**
     * Constructor only for serialisation.
     */
    public Person() {
        super();
    }

    /**
     * Build a new Person instance.
     * 
     * @param name Person name.
     * @param age Person age.
     */
    public Person(String name, int age) {
        super();
        this.name = name;
        this.age = age;
    }

    /**
     * Returns the person's name.
     * 
     * @return The person's name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the person's age.
     * 
     * @return The person's age.
     */
    public int getAge() {
        return this.age;
    }

    /**
     * Sets a new name for the person.
     * 
     * @param name New name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets a new age for the person.
     * 
     * @param age New age.
     */
    public void setAge(int age) {
        this.age = age;
    }

}
