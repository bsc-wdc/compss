package threadConcurrent;

import java.io.Serializable;


public class Container implements Serializable {

    private static final long serialVersionUID = 1L;
    int value;


    public Container() {
        this.value = 0;
    }

    public Container(int value) {
        this.value = value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }

}
