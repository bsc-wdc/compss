package types;

import java.io.Serializable;


public class Pair implements Serializable {

    /**
     * Serializable object outside the runtime
     */
    private static final long serialVersionUID = 3L;

    private int x;
    private int y;


    public Pair() {
        this.x = 0;
        this.y = 0;
    }

    public Pair(int x, int y) {
        this.x = x;
        this.y = y;
    }
    
    public Pair(Pair p) {
        this.x = p.getX();
        this.y = p.getY();
    }

    public int getX() {
        return x;
    }

    public int getY() {
        return y;
    }

    public void setX(int x) {
        this.x = x;
    }

    public void setY(int y) {
        this.y = y;
    }

}
