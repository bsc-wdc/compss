package model;

import java.io.Serializable;


public class Computer implements Serializable {

    /**
     * Serial ID for Objects outside the runtime
     */
    private static final long serialVersionUID = 3L;

    private String brand;
    private String model;
    private String id;
    private int old;


    public Computer(String brand, String model, String id, int old) {
        this.brand = brand;
        this.model = model;
        this.id = id;
        this.old = old;
    }

    public String getBrand() {
        return brand;
    }

    public String getModel() {
        return model;
    }

    public String getId() {
        return id;
    }

    public int getOld() {
        return old;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setOld(int old) {
        this.old = old;
    }

}
