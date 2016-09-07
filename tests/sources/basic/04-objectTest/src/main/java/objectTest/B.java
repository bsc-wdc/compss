package objectTest;

import java.io.Serializable;


@SuppressWarnings("serial")
public class B implements Serializable {

    public String stringField;


    public B() {
    }

    public B(String s) {
        stringField = s;
    }

    public String getStringField() {
        return stringField;
    }

    public void setStringField(String s) {
        stringField = s;
    }

    public String toString() {
        return stringField;
    }

}
