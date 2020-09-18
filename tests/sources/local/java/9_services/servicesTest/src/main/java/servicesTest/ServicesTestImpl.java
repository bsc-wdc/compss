package servicesTest;

import groupservice.Person;


public class ServicesTestImpl {

    public static void print(Person p) {
        System.out.println("[LOG] ---- Name:          " + p.getName());
        System.out.println("[LOG] ---- Surname:       " + p.getSurname());
        System.out.println("[LOG] ---- DNI:           " + p.getDni());
        System.out.println("[LOG] ---- Age:           " + p.getAge());
        System.out.println("[LOG] ---- Production:    " + p.getProduction());
        System.out.println("[LOG] ---- Working Hours: " + p.getWorkingHours());
    }

    public static Person createPerson() {
        Person p = new Person();
        p.setName("You");
        p.setSurname("You");
        p.setDni("87654321Z");
        p.setAge(25);
        p.setProduction(5);
        p.setWorkingHours(2);

        System.out.println("[LOG] ---- Name:          " + p.getName());
        System.out.println("[LOG] ---- Surname:       " + p.getSurname());
        System.out.println("[LOG] ---- DNI:           " + p.getDni());
        System.out.println("[LOG] ---- Age:           " + p.getAge());
        System.out.println("[LOG] ---- Production:    " + p.getProduction());
        System.out.println("[LOG] ---- Working Hours: " + p.getWorkingHours());

        return p;
    }

}
