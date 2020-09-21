package objectStreamTest;

import java.io.Serializable;


public class MyObject implements Serializable {

    // Serializable objects on test have serial version 3L
    private static final long serialVersionUID = 3L;

    private final String name;
    private final int value;


    public MyObject(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return this.name;
    }

    public int getValue() {
        return this.value;
    }

}
