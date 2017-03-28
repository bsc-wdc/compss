package utils;

public class GenericObject {

    private Integer value;


    public GenericObject() {
        this.value = 0;
    }

    public GenericObject(Integer value) {
        this.value = value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return this.value;
    }

}
