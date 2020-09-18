package objectDeregister;

import java.io.Serializable;


public class Dummy implements Serializable {

    private static final long serialVersionUID = 4L;

    private int dummyNumber;


    public Dummy() {

    }

    public Dummy(int number) {
        this.dummyNumber = number;
    }

    public void setDummyNumber(int number) {
        this.dummyNumber = number;
    }

    public int getDummyNumber() {
        return this.dummyNumber;
    }
}
