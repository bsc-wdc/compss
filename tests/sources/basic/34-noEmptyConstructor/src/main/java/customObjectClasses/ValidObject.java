package customObjectClasses;

public class ValidObject {

    private final int id;
    private int name;


    public ValidObject() {
        this.id = 0;
    }

    public ValidObject(int id) {
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
