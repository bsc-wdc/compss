package checkpointObject;

import java.io.Serializable;


public class SimpObj implements Serializable {

    int counter;
    String chars;


    public SimpObj() {
    }

    public SimpObj(int counter, String chars) {
        this.counter = counter;
        this.chars = chars;
    }

    public int getCounter() {
        return counter;
    }

    public String getChars() {
        return chars;
    }

    public void setChars(String chars) {
        this.chars = chars;
    }

    public void setCounter(int counter) {
        this.counter = counter;
    }
}
