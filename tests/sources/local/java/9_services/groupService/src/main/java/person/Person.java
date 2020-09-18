package person;

public class Person {

    private String name;
    private String surname;
    private String dni;
    private int age;
    private int production;
    private int workingHours;


    // Constructors
    public Person() {
        name = null;
        surname = null;
        dni = null;
        production = 0;
        workingHours = -1;
    }

    public Person(String str1, String str2, String str3, int n1, int n2, int n3) {
        name = str1;
        surname = str2;
        dni = str3;
        age = n1;
        production = n2;
        workingHours = n3;
    }

    public Person(Person p) {
        name = p.getName();
        surname = p.getSurname();
        dni = p.getDni();
        age = p.getAge();
        production = p.getProduction();
        workingHours = p.getWorkingHours();
    }

    // Getters
    public String getName() {
        return name;
    }

    public String getSurname() {
        return surname;
    }

    public String getDni() {
        return dni;
    }

    public int getAge() {
        return age;
    }

    public int getProduction() {
        return production;
    }

    public int getWorkingHours() {
        return workingHours;
    }

    // Setters
    public void setName(String str) {
        name = str;
    }

    public void setSurname(String str) {
        surname = str;
    }

    public void setDni(String str) {
        dni = str;
    }

    public void setAge(int n) {
        age = n;
    }

    public void setProduction(int n) {
        production = n;
    }

    public void setWorkingHours(int n) {
        workingHours = n;
    }

    // Others
    public double productivity() {
        return ((double) production) / ((double) workingHours);
    }
}
