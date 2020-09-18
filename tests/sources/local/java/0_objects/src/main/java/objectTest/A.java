package objectTest;

import java.io.Serializable;


@SuppressWarnings("serial")
public class A implements Serializable {

    private int intField;


    public A() {
    }

    public A(int i) {
        intField = i;
    }

    public int getIntField() {
        return intField;
    }

    public void setIntField(int i) {
        intField = i;
    }

    public Integer getAndSetIntField(int newValue) {
        int oldValue = intField;
        intField = newValue;
        return new Integer(oldValue);
    }

    public int square() {
        return intField * intField;
    }

    public String toString() {
        return intField + "";
    }

}
