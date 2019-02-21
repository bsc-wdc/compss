package objectTest;

import java.io.Serializable;


@SuppressWarnings("serial")
public class C implements Serializable {

    private A aField;
    private B bField;


    public C() {
    } // Necessary when returning an object of type C from a task

    public C(A a, B b) {
        aField = a;
        bField = b;
        System.out.println("In C constructor, creating C with A " + aField + " and B " + bField);
    }

    public A getAField() {
        return aField;
    }

    public B getBField() {
        return bField;
    }

    public void setAField(A a) {
        aField = a;
    }

    public void setBField(B b) {
        bField = b;
    }

    public String toString() {
        return "C with A " + aField + " and B " + bField;
    }
}
