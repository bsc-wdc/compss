package customObjectClasses;

public class InvalidObject {

    private final int id;
    private int name;


    public InvalidObject(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setName(int name) {
        this.name = name;
    }

    public int getName() {
        return this.name;
    }

}
